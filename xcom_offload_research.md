# XCom Offload через XComObjectStorageBackend, текущее состояние

## Механизм XComObjectStorageBackend

### Расположение

```
providers/common/io/src/airflow/providers/common/io/xcom/backend.py
```

### Конфигурация

```ini
[core]
xcom_backend = airflow.providers.common.io.xcom.backend.XComObjectStorageBackend

[common.io]
xcom_objectstorage_path = s3://conn_id@mybucket/key
xcom_objectstorage_threshold = 0   # всегда offload в хранилище
xcom_objectstorage_compression = gzip    # опционально
```

`xcom_objectstorage_path` поддерживает любую схему, которую понимает `fsspec`:
`s3://`, `gs://`, `abfs://`, `file://`, `local://` и т.д.

Connection ID берётся из **user-части URL**: `scheme://conn_id@bucket/path`.

### Логика serialize_value

Значение сериализуется через `json.dumps` с `XComEncoder`. Если размер в байтах меньше `threshold`, то сохраняется в БД напрямую через `BaseXCom.serialize_value`. Если больше или равно, то записывается в ObjectStorage по пути `<base_path>/<dag_id>/<run_id>/<task_id>/<uuid>[.suffix]`, а в БД сохраняется строка с этим путём

### Логика deserialize_value

Читает значение из БД. Если это не URL, то возвращает как есть. Если URL и он относителен к `base_path`, то читает файл из ObjectStorage и десериализует через `json.load` с `XComDecoder`

### Кеширование конфига

Функции `_get_base_path()`, `_get_threshold()`, `_get_compression()` декорированы `@cache`,
значения читаются из конфига **один раз** при первом вызове и кешируются на весь процесс

## Интеграция с внешними хранилищами

`XComObjectStorageBackend` сам по себе не знает ни про S3, ни про HDFS. Он работает через `ObjectStoragePath` из task-sdk, который является обёрткой над [fsspec](https://filesystem-spec.readthedocs.io/). Конкретная файловая система определяется по схеме URL (`s3://`, `hdfs://` и т.д.) и подключается через механизм провайдеров Airflow

Каждый провайдер, желающий поддержать свою схему, регистрирует модуль с функцией `get_fs(conn_id, storage_options) -> AbstractFileSystem` в своём `provider.yaml` под ключом `filesystems`. При первом обращении к хранилищу Airflow находит нужный провайдер по схеме, вызывает его `get_fs`, передавая `conn_id` извлечённый из URL (`s3://my_conn@bucket/path` -> `conn_id = "my_conn"`), и получает готовый fsspec-объект

Далее все операции (`open`, `mkdir`, `exists`, `unlink`) идут через стандартный fsspec API, `XComObjectStorageBackend` об этом не знает

## Реализация поддержки S3

S3-поддержка реализована в `providers/amazon/src/airflow/providers/amazon/aws/fs/s3.py` и регистрирует схемы `s3`, `s3a`, `s3n`

Функция `get_fs` берёт `conn_id`, создаёт `S3Hook` для чтения Airflow Connection, и возвращает `S3FileSystem`

## Реализация поддержки Ozone через WebHDFS

### Контекст

Apache Ozone предоставляет HTTP-совместимый интерфейс через HttpFS (порт 14001)
Стандартный `XComObjectStorageBackend` не подходит напрямую, потому что fsspec реализует
WebHDFS upload в два шага:
1. POST для создания файла, сервер отвечает redirect URL
2. PUT на redirect URL с данными

Ozone HttpFS этот протокол не поддерживает. Вместо этого требует один PUT запрос
с параметрами `op=CREATE&data=true` напрямую на URL файла с данными в теле запроса

### Патч WebHDFile

Реализован в `providers/arenadata/ozone/src/airflow/providers/arenadata/ozone/fs/webhdfs.py`

Класс `_OzoneWebHDFile` переопределяет два метода:
- `_initiate_upload`: no-op, пропускаем первый шаг
- `_upload_chunk`: единственный PUT с `op=CREATE&data=true`

Патч применяется на уровне класса `WebHDFile` через `_make_ozone_webhdfs`,
которая вызывается один раз при создании `WebHDFS` клиента в `get_fs()`.
Патч класса гарантирует что все последующие инстансы `WebHDFile` получат нужные методы

### Кастомный XCom backend

`XComHdfsBackend(XComObjectStorageBackend)` реализован в
`providers/arenadata/ozone/src/airflow/providers/arenadata/ozone/xcom/hdfs_backend.py`

Кастомный backend нужен по двум причинам:
- Ozone не принимает `:` и `+` в путях, нужна санитизация `run_id`
- Ozone не поддерживает двухшаговый WebHDFS upload, нужен патч `WebHDFile`

### Конфигурация

```ini
[core]
xcom_backend = airflow.providers.arenadata.ozone.xcom.backend.XComOzoneBackend

[common.io]
xcom_objectstorage_path = webhdfs://ozone_webhdfs_default@/vol1/bucket-legacy/xcom
xcom_objectstorage_threshold = 1
```

### Регистрация fs провайдера

`get_fs` зарегистрирован в `provider.yaml` под ключом `filesystems` со схемой `webhdfs`
Папка `fs/` это стандартное соглашение Airflow провайдеров для fsspec-реализаций

Регистрация нужна чтобы `ObjectStoragePath("webhdfs://...")` нашел пропатченный `get_fs`
через `_register_filesystems()` в `airflow.sdk.io.fs`. Без неё использовался бы стандартный
fsspec `WebHDFS` без патча, и запись в Ozone сломалась бы на двухшаговом upload


## Реализация поддержки HDFS через HttpFS

### Контекст

Apache HDFS предоставляет HTTP-интерфейс через HttpFS (порт 14000)
В отличие от Ozone, HDFS HttpFS поддерживает стандартный двухшаговый WebHDFS протокол:
1. PUT для создания файла, сервер отвечает redirect 307 на DataNode
2. PUT на redirect URL с данными

Также поддерживает и одношаговый протокол. Стандартный fsspec `WebHDFS` работает с HDFS без патчей. Однако `XComObjectStorageBackend`
не подходит напрямую из-за проблемы с `run_id`

### Проблема run_id в HDFS путях

`run_id` в Airflow содержит символы `:` и `+` (например `manual__2024-01-15T10:30:00+00:00`)
HDFS считает такие символы невалидными в именах файлов и возвращает ошибку:
`Pathname /xcom/.../manual__2024-01-15T10:30:00+00:00/... is not a valid DFS filename`

`XComObjectStorageBackend` не санитизирует `run_id` перед формированием пути, это его ограничение

### Кастомный XCom backend

`XComHdfsBackend(XComObjectStorageBackend)` реализован в
`providers/arenadata/ozone/src/airflow/providers/arenadata/ozone/xcom/hdfs_backend.py`

`serialize_value` единственный переопределённый метод:
- Санитизирует `run_id` через `_safe()` перед передачей в родительский класс
- Делегирует всё остальное в `XComObjectStorageBackend.serialize_value`

`deserialize_value` не переопределяется, родительский метод работает корректно,
путь в БД содержит уже санитизированный `run_id`, чтение файла проходит без проблем

`_safe(value)` заменяет все символы кроме `\w`, `.`, `-` на `_`

### Конфигурация

```ini
[core]
xcom_backend = airflow.providers.arenadata.ozone.xcom.hdfs_backend.XComHdfsBackend

[common.io]
xcom_objectstorage_path = webhdfs://hdfs_default@/xcom
xcom_objectstorage_threshold = 0
```

### Патч WebHDFile применяется и для HDFS

`get_fs` из `webhdfs.py` патчит `WebHDFile` глобально через `_make_ozone_webhdfs`
для любого `webhdfs://` соединения, включая HDFS. HDFS HttpFS принимает одношаговый
PUT с `data=true` наравне со стандартным двухшаговым протоколом, поэтому патч не ломает HDFS

### Итог

Минимальный backend решает проблему санитизации `run_id`,  вся остальная логика (fsspec, ObjectStoragePath, threshold, compression) наследуется
из `XComObjectStorageBackend` без изменений

## Реализация поддержки Ozone через нативный OFS (RootedOzoneFileSystem)

### Контекст

Apache Ozone поддерживает нативный файловый протокол `ofs://` через `RootedOzoneFileSystem`
OFS может работать через Ozone CLI (`ozone fs`), который использует
Java-клиент напрямую без HTTP-прослойки. Схема `ofs://` позволяет адресовать несколько
volume и bucket в одном пути: `ofs://om-address/volume/bucket/key`

### Конфигурация

```ini
[core]
xcom_backend = airflow.providers.arenadata.ozone.xcom.hdfs_backend.XComHdfsBackend

[common.io]
xcom_objectstorage_path = ozoneofs://ozoneofs_default@s3v/ofs-bucket/xcom
xcom_objectstorage_threshold = 1
```

Connection `ozoneofs_default`, тип `generic`, `host=ozone`, `port=9862` (Ozone Manager)

### Плюсы подхода

- Нет зависимости от Java/Hadoop в Python-коде, только `subprocess`
- Работает с любой версией Ozone без привязки к версии клиентских библиотек
- `ofs://` даёт доступ к нескольким volume/bucket в одном пути
- Простота реализации, минимум кода

### Минусы подхода

- JVM на каждый subprocess это 300–800 мс только на инициализацию процесса
- Tempfile на диске это двойное копирование данных при каждой операции
- Нет стриминга — весь файл читается/пишется целиком в память
- Нет connection pooling — каждый вызов устанавливает новое соединение с OM
- Хрупкость — зависимость от формата вывода CLI и кодов возврата
- `ozone` бинарник должен быть доступен в `PATH` на каждом worker-узле

### Применимость

Для XCom backend подход приемлем: операций мало (один `put` на serialize, один `get` на deserialize), файлы маленькие (десятки байт). JVM overhead незаметен на фоне времени выполнения тасков. Для general-purpose filesystem с частыми или большими операциями подход неприемлем

## Исследование Python-клиентов для Ozone через HDFS-протокол

### Контекст

Ozone Manager слушает порт 9862 и принимает Hadoop RPC. Возникла гипотеза: раз Ozone
поддерживает HDFS-совместимый интерфейс, возможно Python-клиенты для HDFS смогут с ним работать.

### hdfs-native (Rust)

Библиотека на Rust, реализует Hadoop RPC клиент. Подключается к порту 9862, но получает:

```
RpcNoSuchProtocolException: Unknown protocol: org.apache.hadoop.hdfs.protocol.ClientNamenodeProtocol
```

### snakebite-py3

Pure Python реализация HDFS клиента. Та же ошибка:

```
RpcNoSuchProtocolException: Unknown protocol: org.apache.hadoop.hdfs.protocol.ClientNamenodeProtocol
```

### Причина несовместимости

Ozone Manager реализует `OzoneManagerProtocol`, а не `ClientNamenodeProtocol`.
Это принципиально разные протоколы, подтверждено через `unzip -l` на jar-файлах в контейнере:

- `ClientNamenodeProtocol` — в `hadoop-hdfs-client-3.4.3.jar` и `hadoop-hdfs-3.4.3.jar` (HDFS NameNode)
- `OzoneManagerProtocol` — в `ozone-interface-client-2.2.1.jar` (Ozone Manager)

Оба `hdfs-native` и `snakebite-py3` реализуют `ClientNamenodeProtocol` и несовместимы с Ozone OM
Для работы с Ozone через RPC нужна реализация `OzoneManagerProtocol`, которой в Python-экосистеме нет

## Реализация поддержки Ozone через PyArrow HadoopFileSystem

`OzoneArrowFileSystem(AbstractFileSystem)` в `fs/ofs_arrow.py`, схема `ozonearrow://`, делегирует в `ArrowFSWrapper(HadoopFileSystem("default"))`

### Конфигурация

```ini
[core]
xcom_backend = airflow.providers.arenadata.ozone.xcom.hdfs_backend.XComHdfsBackend

[common.io]
xcom_objectstorage_path = ozonearrow://ozonearrow_default@s3v/ofs-bucket/xcom
xcom_objectstorage_threshold = 1
```

Connection `ozonearrow_default`, тип `generic`, `host=ozone`, `port=9862`

### Итог

Лучший из реализованных подходов: нативный протокол Ozone, JVM инициализируется один раз на процесс, стриминг без tempfile. Требует Java и `core-site.xml` на каждом worker-узле

| Подход | Протокол | JVM overhead | Стриминг |
|---|---|---|---|
| WebHDFS (HttpFS) | HTTP | нет | нет |
| subprocess (ozone fs) | OzoneManagerProtocol | на каждый вызов | нет |
| PyArrow + libhdfs | OzoneManagerProtocol | один раз на процесс | да |
| hdfs-native / snakebite | ClientNamenodeProtocol | — | несовместимо |

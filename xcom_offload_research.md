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

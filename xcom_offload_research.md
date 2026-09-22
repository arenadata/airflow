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
xcom_objectstorage_threshold = 1048576   # байт, если -1, то offload всегда в БД
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


## DAG-тесты

### test_xcom_offload.py, TaskFlow (неявный XCom)

Таск `produce` возвращает `LARGE_PAYLOAD` через `return`. TaskFlow автоматически вызывает `xcom_push(key="return_value", ...)`. Таск `consume` получает значение как аргумент, TaskFlow вызывает `xcom_pull` до входа в функцию

### test_xcom_offload_explicit.py, явный xcom_push/xcom_pull

Таск `produce` вызывает `ti.xcom_push(key="my_data", value=LARGE_PAYLOAD)` явно. Таск `consume` вызывает `ti.xcom_pull(task_ids="produce", key="my_data")` явно и проверяет наличие файла в object storage

### Проблема кеширования и её решение

`_get_base_path()` и `_get_threshold()` декорированы `@cache`. При запуске через UI модуль DAG-файла может быть уже закеширован в памяти воркера, с момента парсинга планировщиком, когда env vars ещё не были выставлены

Решение: вызывать `cache_clear()` внутри каждого таска. Тогда кеш сбрасывается при каждом выполнении таска, независимо от состояния модуля



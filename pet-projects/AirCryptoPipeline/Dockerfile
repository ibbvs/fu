# Возьмем за основу данный образ
# Данная команда установит уже готовый образ с python:3.8
# После чего поверх него установит дополнительные 
# команды которые мы укажем дальше
FROM python:3.8 

# Устанавливаем домашнюю директорию внутри контейнера
# При первом запуске Airflow в каталоге $AIRFLOW_HOME
# будет создан файл airflow.cfg.
ENV AIRFLOW_HOME=/usr/local/airflow

# Airflow глобальные переменные
ARG AIRFLOW_VERSION=2.1.4

# Здесь и выше мы использовали глобальные переменные
# Они нужны чтобы не прописывать каждый раз что-то в 
# config файл, это проще и гибче, синтаксис следующий
# AIRFLOW__{SECTION}__{KEY} конкретные данные нужно искать в документации

# Папка с дагами и плагинами
ENV AIRFLOW__CORE__DAGS_FOLDER=/usr/local/airflow/dags 
ENV AIRFLOW__CORE__PLUGINS_FOLDER=/usr/local/airflow/plugins

# Заменим executor
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor

# Отключим примеры кода
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Установка airflow
RUN pip install apache-airflow==${AIRFLOW_VERSION}

# Установка визуального редактора для работы в Airflow UI
# Дополнительная настройка, чтобы можно было редактировать код
# Внутри веб интерфейса
RUN pip install airflow-code-editor
RUN pip install black fs-s3fs fs-gcsfs

# Слздадим отдельную папку для нашего скрипта запуска
# Затем скопируем сам скрипт и после чего выдадим расширенные права
RUN mkdir /project
COPY scripts/ /project/scripts/
RUN chmod +x /project/scripts/init.sh

# Запускаем sh скрипт
# Начнет процесс инициализации airflow
ENTRYPOINT ["/project/scripts/init.sh"]

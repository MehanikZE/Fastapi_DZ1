Для работы программы необходимо иметь установленную СУБД Postgresql
По умолчанию настроено на следующие данные:
DB_USER = "postgres"
DB_NAME = "test"
DB_PASSWORD = "Vrt342zf"
DB_HOST = "127.0.0.1"
При использовании других учетных данных к БД, необходимо скорректировать в разделе "Подключение к БД"
Также необходимо наличие модулей из requirements.txt
Программа запускается с помощью команды uvicorn main:app
После запуска происходит создание таблиц и наполнение их параметрами для дальнейшего взаимодествия с ними, дабы упростить проверку функционала приложения, и его демонстрации асинхронного ввода вывода в БД,
если планируется перезапуск приложения то код наполнения данными нужно закомментировать, поскольку некоторые поля таблиц имеют уникальность и не должны повторяться, соответственно приложение не даст запуск сервера в таком случае.
Для теста необходимо зайти на API http://127.0.0.1:8000/docs либо использовать свой агент для запросов.
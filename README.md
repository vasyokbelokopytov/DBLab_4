# DBLab_4

## Лабораторна робота №4 з баз даних

Бєлокопитов В. О., Група КМ-83, Варіант 3
Для запуску програми необхідна встановлена [MongoDB](https://www.mongodb.com/try/download/community), а також python не нижче 3-ї версії.

## Інструкція по запуску:

1. Створити віртуальне середовище наступними командами:
   `python -m pip install virtualenv`
   `python -m venv venv`
   `source ./venv/Scripts/activate`
   `python -m pip install -r requirements.txt`
2. Додати у папку `data` два файли з назвами в форматі `OdataXXXXFile.csv`
3. Запустити `MongoDB`
4. Запусити команду `python main.py`

Програма запише дані в базу даних, виконає запит, та запише результат у файл `result.csv`.
Логи будуть доступні у файлі `logs.log`

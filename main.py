from __future__ import print_function
import gspread
import csv, shutil
#авторизируемся на сервере с помощью email и private_key, который получили в GCP
gc = gspread.service_account(filename='keys.json')
#Ключ таблицы с которой нужно считать данные
sh_read = gc.open_by_key("1CCEd2_KlXeNzWFAo_FX09UCXJ2GTXSWqbzKLtIu8TNU")

lists_table = ["leads", "managers", "clients", "transactions"]
#
for worksheet_list in lists_table:
    #выбираем лист книги
    worksheet = sh_read.worksheet(worksheet_list)
    #считываем значения в виде списка списков
    dataframe = worksheet.get_all_values()
    #создаём файл с данными по листам
    with open(f"csv/{worksheet_list}.csv", "w", newline=''.format(worksheet_list)) as files:
        writer = csv.writer(files, delimiter=';')
        writer.writerows(dataframe)
#Создаём связующую таблицу на основе таблицы leads.csv
shutil.copyfile("csv/leads.csv", "csv/main.csv")
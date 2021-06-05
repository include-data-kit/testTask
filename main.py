from __future__ import print_function
import gspread
import csv
import shutil
import pandas as pd
import datetime as dt

def auth_reading_csv():
    #Авторизируемся на сервере с помощью email и private_key, который получили в GCP
    gc = gspread.service_account(filename='keys.json')
    #Ключ таблицы с которой нужно считать данные
    sh_read = gc.open_by_key("1CCEd2_KlXeNzWFAo_FX09UCXJ2GTXSWqbzKLtIu8TNU")

    lists_table = ["leads", "managers", "clients", "transactions"]

    for worksheet_list in lists_table:
        #Выбираем лист книги
        worksheet = sh_read.worksheet(worksheet_list)
        #Считываем значения в виде списка списков
        dataframe = worksheet.get_all_values()
        #Создаём файл с данными по листам
        with open(f"csv/{worksheet_list}.csv", "w", newline=''.format(worksheet_list)) as files:
            writer = csv.writer(files, delimiter=';')
            writer.writerows(dataframe)
        # Создаём связующую таблицу на основе таблицы leads.csv
        shutil.copy("csv/leads.csv", "csv/main.csv")

def managers_list():
    df1 = pd.read_csv('csv/main.csv', delimiter=';')
    df2 = pd.read_csv('csv/managers.csv', delimiter=';')
    #Забираем Значения d_club и d_manager из managers.csv по manager_id
    for index_1, item_manager_id in enumerate(df1['l_manager_id']):
        for index_2, item_manager in enumerate(df2['manager_id']):
            if item_manager_id == item_manager:
                df1.at[index_1, 'd_manager'] = df2.at[index_2, 'd_manager']
                df1.at[index_1, 'd_club'] = df2.at[index_2, 'd_club']
            elif item_manager_id == "00000000-0000-0000-0000-000000000000":
                continue

    df1.to_csv('csv/main.csv', sep=';', index=False)

def transactions_list():
    df1 = pd.read_csv('csv/main.csv', delimiter=';')
    df2 = pd.read_csv('csv/transactions.csv', delimiter=';')
    for index_1, item_client_id in enumerate(df1['l_client_id']):
        for index_2, item_client in enumerate(df2['l_client_id']):
            if item_client_id == "00000000-0000-0000-0000-000000000000":
                break
            elif item_client_id == item_client:
                if dt.datetime.strptime(df1.at[index_1, 'created_at'][:10], '%Y-%m-%d') <= dt.datetime.strptime(df2.at[index_2, 'created_at'][:10], '%Y-%m-%d'): # Проверка даты Main.cvs и transactions.csv, если дата лида меньше или равна дате транзакции
                    df1.at[index_1, 'transaction_created_at'] = df2.at[index_2, 'created_at']
                    df1.at[index_1, 'm_real_amount'] = df2.at[index_2, 'm_real_amount']
                    break
                elif dt.datetime.strptime(df1.at[index_1, 'created_at'][:10], '%Y-%m-%d') > dt.datetime.strptime(df2.at[index_2, 'created_at'][:10], '%Y-%m-%d'):
                    df1.at[index_1, 'transaction_created_at'] = ''
                    df1.at[index_1, 'm_real_amount'] = '0'
                    continue

    df1.to_csv('csv/main.csv', sep=';', index=False)

def check_new_client():
    df1 = pd.read_csv('csv/main.csv', delimiter=';')
    df2 = pd.read_csv('csv/clients.csv', sep=';')

    for index_1, item_client_id in enumerate(df1['l_client_id']):
        for index_2, item_client in enumerate(df2['client_id']):
            if item_client_id == item_client:
                #Срезаем время от даты
                create_lead = df1.at[index_1, 'created_at'][:10].split('-')

                if df2.at[index_2, 'created_at'] == "0001-01-01 00:00:00":
                    break

                create_client = df2.at[index_2, 'created_at'][:10].split('-')

                #Преобразуем строку с датой в числовые значения
                create_lead = [int(item) for item in create_lead]
                create_client = [int(item) for item in create_client]
                #Преобразуем числа в дату в формате /Y/M/D
                create_lead_date = dt.date(create_lead[0], create_lead[1], create_lead[2])
                create_client_date = dt.date(create_client[0], create_client[1], create_client[2])
                #Вычисляем розницу в днях
                diff_days = create_lead_date - create_client_date
                #Узнаём разницу в днях
                diff_days = diff_days.days

                diff_days = int(diff_days)
                #Если клиент новый, то 1
                if diff_days == 0:
                    df1.at[index_1, 'new_client'] = '1'
                else:
                    df1.at[index_1, 'new_client'] = '0'
                break

            elif item_client_id == "00000000-0000-0000-0000-000000000000":
                break

    df1.to_csv('csv/main.csv', sep=';', index=False)

def new_customers():
    df1 = pd.read_csv('csv/main.csv', delimiter=';')

    for index_1, item_client_id in enumerate(df1['l_client_id']):

        if pd.isna(df1.at[index_1, 'transaction_created_at']) == True:
                df1.at[index_1, 'new_customer'] = '0'
        else:
            create_lead = df1.at[index_1, 'created_at'][:10].split('-')

            new_customer = df1.at[index_1, 'transaction_created_at'][:10].split('-')

            create_lead = [int(item) for item in create_lead]
            new_customer = [int(item) for item in new_customer]

            create_lead_date = dt.date(create_lead[0], create_lead[1], create_lead[2])
            new_customer_date = dt.date(new_customer[0], new_customer[1], new_customer[2])

            diff_days = create_lead_date - new_customer_date

            diff_days = diff_days.days

            diff_days = int(diff_days)

            if 0 <= diff_days <= 7:
                df1.at[index_1, 'new_customer'] = '1'
            else:
                df1.at[index_1, 'new_customer'] = '0'

    df1.to_csv('csv/main.csv', sep=';', index=False)

def main():
    auth_reading_csv()
    managers_list()
    transactions_list()
    check_new_client()
    new_customers()

if __name__ == "__main__":
    main()
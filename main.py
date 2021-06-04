from __future__ import print_function
import gspread
import csv, shutil
import pandas as pd

def auth_reading_csv():
    #авторизируемся на сервере с помощью email и private_key, который получили в GCP
    gc = gspread.service_account(filename='keys.json')
    #Ключ таблицы с которой нужно считать данные
    sh_read = gc.open_by_key("1CCEd2_KlXeNzWFAo_FX09UCXJ2GTXSWqbzKLtIu8TNU")

    lists_table = ["leads", "managers", "clients", "transactions"]

    for worksheet_list in lists_table:
        #выбираем лист книги
        worksheet = sh_read.worksheet(worksheet_list)
        #считываем значения в виде списка списков
        dataframe = worksheet.get_all_values()
        #создаём файл с данными по листам
        with open(f"csv/{worksheet_list}.csv", "w", newline=''.format(worksheet_list)) as files:
            writer = csv.writer(files, delimiter=';')
            writer.writerows(dataframe)
        # Создаём связующую таблицу на основе таблицы leads.csv
        shutil.copyfile("csv/leads.csv", "csv/main.csv")

def managers_list():
    df1 = pd.read_csv('csv/main.csv', delimiter=';')
    df2 = pd.read_csv('csv/managers.csv', delimiter=';')
    #Забираем Значения d_club и d_manager из managers.csv по manager_id
    for index_1, item_manager_id in enumerate(df1['l_manager_id']):
        for index_2, item_manager in enumerate(df2['manager_id']):
            if item_manager_id == item_manager:
                df1.at[index_1,'d_manager'] = df2.at[index_2, 'd_manager']
                df1.at[index_1, 'd_club'] = df2.at[index_2, 'd_club']
            elif item_manager_id == "00000000-0000-0000-0000-000000000000":
                continue

    df1.to_csv('csv/main.csv', sep=';', index=False)

def transactions_list():
    df1 = pd.read_csv('csv/main.csv', delimiter=';')
    df2 = pd.read_csv('csv/transactions.csv', delimiter=';')

    for index_1, item_client_id in enumerate(df1['l_client_id']):
        for index_2, item_client in enumerate(df2['l_client_id']):
            if item_client_id == item_client:
                df1.at[index_1, 'transaction_created_at'] = df2.at[index_2, 'created_at']
                df1.at[index_1, 'm_real_amount'] = df2.at[index_2, 'm_real_amount']
            elif item_client_id == "00000000-0000-0000-0000-000000000000":
                continue
    df1.to_csv('csv/main.csv', sep=';', index=False)

def check_new_client():
    df1 = pd.read_csv('csv/main.csv', delimiter=';')
    df2 = pd.read_csv('csv/clients.csv', delimiter=';')

    for index_1, item_client_id in enumerate(df1['l_client_id']):
        for index_2, item_client in enumerate(df2['client_id']):
            if item_client_id == item_client:
                df1.at[index_1, 'old_client'] = '1'
                break
            elif item_client_id == "00000000-0000-0000-0000-000000000000":
                continue
            else:
                df1.at[index_1, 'old_client'] = '0'
        if index_1 == 1000:
            break
    df1.to_csv('csv/main.csv', sep=';', index=False)

def main():
    auth_reading_csv()
    managers_list()
    transactions_list()
    check_new_client()

if __name__ == "__main__":
    main()
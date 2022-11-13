import os
import time
import uuid

import pandas
import logging

import pymssql
import schedule
from deepdiff import DeepDiff

from dotenv import load_dotenv, find_dotenv
from schedule import every, repeat, run_pending

load_dotenv(find_dotenv())


@repeat(every(15).seconds)
def job():
    print("I'm working...")
    # Connect to db

    conn = pymssql.connect(server='localhost', user='sa', password='Handx123', database='policy-history')

    cur = conn.cursor()

    # Sql get news list
    sql01 = """
        ;with cte as (select concat([Policy Number], [Coverage ID], [Premium Payment Frequency], [Main Rider Product Name]) as ID_OLD from OP01_OLD)
            select o.[Policy Number], o.[Coverage ID], o.[Premium Payment Frequency], o.[Main Rider Product Name] from OP01 o
                left join cte on concat(o.[Policy Number], o.[Coverage ID], o.[Premium Payment Frequency], o.[Main Rider Product Name]) = cte.ID_OLD
            where cte.ID_OLD is null
        """

    # Sql get remove list
    sql02 = """
       ;with cte as (select concat([Policy Number], [Coverage ID], [Premium Payment Frequency], [Main Rider Product Name]) as ID_NEW from OP01)
        select o.[Policy Number], o.[Coverage ID], o.[Premium Payment Frequency], o.[Main Rider Product Name] from OP01_OLD o
            left join cte on concat(o.[Policy Number], o.[Coverage ID], o.[Premium Payment Frequency], o.[Main Rider Product Name]) = cte.ID_NEW
        where cte.ID_NEW is null
       """

    try:

        c01 = conn.cursor(as_dict=True)
        c02 = conn.cursor(as_dict=True)

        c01.execute(sql01)
        c02.execute(sql02)

        row01 = c01.fetchall()
        row02 = c02.fetchall()

        # 1. Create GUID and update to OLD

        # 2. Get NEW & OLD by GUID

        g = uuid.uuid4()

        sql100 = """
            ;with cte as (
                    select top 100 concat([Policy Number], [Coverage ID], [Premium Payment Frequency], [Main Rider Product Name]) as ID_OLD, type 
                        from OP01_OLD
                    where type = 0 and batch_job is null )
            update OP01_OLD
                set o.batch_job = '52B01C21-7DA0-4117-BBD7-9AB739016C4F'
            from OP01_OLD o
                inner join cte on concat(o.[Policy Number], o.[Coverage ID], o.[Premium Payment Frequency], o.[Main Rider Product Name]) = cte.ID_OLD
        """

        sqlOld = """
            ;select concat(o.[Policy Number], o.[Coverage ID], o.[Premium Payment Frequency], o.[Main Rider Product Name]) as ID_OLD,
                o.[Distribution Channel], o.[Sub Channel], o.[Agent Code], o.[Agent Name],
                o.[Position], o.[Agent 2 Code], o.[Agent 2 Name], o.[Position 2], o.[Branch Name],
                o.[Mã nhân viên MB], o.[Tên nhân viên], o.[Mã người giới thiệu], o.[Tên người giới thiệu], o.[Mã người hỗ trợ],
                o.[Tên người hỗ trợ], o.[Area Name Direct Name], o.[Region DD Name], o.[Main Product],
                o.[Policy Effecitve Date], o.[Policy Holder ID], o.[Poliy holder name], o.[BOD], o.[Policy holder's mobile], o.[Policy holder's email],
                o.[Address], o.[Insured Person ID], o.[Insured's Gender], o.[Insured's DOB], o.[Insured's Name],
                o.[Insured's ID Number], o.[Insured's Occupation], o.[Insured Amount], o.[Policy Term],
                o.[Premium Term], o.[Tariff Premium], o.[Periodic Premium], o.[APE], o.[Relative Value],
                o.[Extra Mortality], o.[Issued User], o.[Issued Date], o.[Issued Date_Year], o.[Reversed Date],
                o.[Application Receipt Date], o.[Application Date], o.[ACK Date], o.[Bank Key of Account], o.[Bank Account],
                o.[Policy Status], o.[Lapse Date], o.[Collection Status], o.[Update_Date],
                o.[Coverage Start Date], o.[Coverage End Date], o.[Premium End Date], o.[Policy Lock], o.[Coverage Status], o.[created_date] 
            from OP01_OLD o
                where o.batch_job = '52B01C21-7DA0-4117-BBD7-9AB739016C4F'
        """

        sqlNew = """
            ;with cte as (
                    select concat([Policy Number], [Coverage ID], [Premium Payment Frequency], [Main Rider Product Name]) as ID_OLD 
                        from OP01_OLD
                        where type = 0 and batch_job = '52B01C21-7DA0-4117-BBD7-9AB739016C4F'
                )
            select cte.ID_OLD,
                o.[Distribution Channel], o.[Sub Channel], o.[Agent Code], o.[Agent Name],
                o.[Position], o.[Agent 2 Code], o.[Agent 2 Name], o.[Position 2], o.[Branch Name],
                o.[Mã nhân viên MB], o.[Tên nhân viên], o.[Mã người giới thiệu], o.[Tên người giới thiệu], o.[Mã người hỗ trợ],
                o.[Tên người hỗ trợ], o.[Area Name Direct Name], o.[Region DD Name], o.[Main Product], 
                o.[Policy Effecitve Date], o.[Policy Holder ID], o.[Poliy holder name], o.[BOD], o.[Policy holder's mobile], o.[Policy holder's email],
                o.[Address], o.[Insured Person ID], o.[Insured's Gender], o.[Insured's DOB], o.[Insured's Name],
                o.[Insured's ID Number], o.[Insured's Occupation], o.[Insured Amount], o.[Policy Term],
                o.[Premium Term], o.[Tariff Premium], o.[Periodic Premium], o.[APE], o.[Relative Value],
                o.[Extra Mortality], o.[Issued User], o.[Issued Date], o.[Issued Date_Year], o.[Reversed Date],
                o.[Application Receipt Date], o.[Application Date], o.[ACK Date], o.[Bank Key of Account], o.[Bank Account],
                o.[Policy Status], o.[Lapse Date], o.[Collection Status], o.[Update_Date],
                o.[Coverage Start Date], o.[Coverage End Date], o.[Premium End Date], o.[Policy Lock], o.[Coverage Status], o.[created_date] 
             from OP01 o
                inner join cte on concat(o.[Policy Number], o.[Coverage ID], o.[Premium Payment Frequency], o.[Main Rider Product Name]) = cte.ID_OLD
        """

        cOld = conn.cursor(as_dict=True)
        cNew = conn.cursor(as_dict=True)

        cOld.execute(sqlOld)
        rowOld = cOld.fetchall()

        cNew.execute(sqlNew)
        rowNew = cNew.fetchall()

        print(DeepDiff(rowOld, rowNew, group_by='ID_OLD', encodings=['utf-8', 'latin-1']))

        conn.commit()
    except Exception as error:
        logging.error("Error: %s" % error)
        conn.rollback()
        cur.close()
    cur.close()
    conn.close()


if __name__ == '__main__':
    # schedule
    while True:
        schedule.run_pending()
        time.sleep(1)

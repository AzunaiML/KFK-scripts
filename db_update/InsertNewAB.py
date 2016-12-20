# -*- coding: windows-1251 -*-

import pandas as pd
import numpy as np
from xml.etree import ElementTree
import shutil
import os.path
import os
import datetime
import calendar
import glob

address = 			'ADDRESS.CSV'
client_dog = 		'CL_CLIENT_DOG.CSV'
client = 			'CL_CLIENTS.CSV'
passport = 			'CL_CLIENTS_PASPORT.CSV'
contact = 			'CL_CONTACT.CSV'
credit = 			'CL_CREDITS.CSV'
zalog = 			'CL_ZALOG.CSV'
zalog_auto = 		'CL_ZALOG_AUTO.CSV'
insurance = 		'INSURANCE.CSV'

sql_script_df_structure = ['file', 'id', 'SQL']

inserts = {
	'address': u"insert akb_Address select {0} CLIENT_ID, '{1}' COUNTRY, '{2}' REGION, '{3}' CITY, '{4}' LOCALITY, '{5}' STREET, '{6}' BUILDING_NUM, '{7}' BLOCK_NUM, '{8}' APARTMENT_NUM, '{9}' ADDRESS_TYPE, '{10}' POST_INDEX",
	
	'client_dog': u"insert akb_client_dog select {2} CLIENT_ID, {0} AGREEMEN_ID, '{1}' CONTR_TYPE",
	
	# FIO - title
	'client': u"insert akb_Clients select {0} CLIENT_ID, '{1}' FIRST_NAME, '{2}' LAST_NAME, '{3}' MIDDLE_NAME,convert(datetime,'{4}', 103) BIRTHDAY,{5} EXIST_CREDIT_CARD_FLAG",
	
	# CLOSE_DATE, OPEN_DATE - if '' then NULL
	'passport': u"insert akb_clients_pasport select {0} CLIENT_ID, '{1}' DOC_TYPE, '{2}' DOC_SERIES, '{3}' DOC_NUMBER, '{4}' DEP_CODE, '{5}' ISSUE_PLACE, convert(smalldatetime,{6},103) OPEN_DATE, convert(smalldatetime,{7},103) CLOSE_DATE, '{8}' REASON, '{9}' COMMENTES",
	
	'contact': u"insert akb_contact select {0} CLIENT_ID, '{2}' CONTACT_TYPE, '{1}' CONTACT_VALUE",
	
	# ANNUITY, OVERDUE_DAY - if '' then 0
	# BRANCH_NAME - if '' then NULL
	'credit': u"insert akb_credits select {0} #AGREEMENT_ID, '{1}' AGREEMENT_NUM, {2} AGREEMENT_LIMIT, {3} TOOL_ISO_CODE, {4} ANNUITY, {5} CLIENT_ID, convert(smalldatetime,'{6}', 103) SIGN_DATE, {7} OVERDUE_DAY,'{8}' PRODUCT_NAME,'{9}' PRODUCT_TYPE_NAME, '{10}' BRANCH_NAME",
	
	# ROOMS_COUNT, FLOORSPACE, PLEDGE_AGREEMENT_VALUE - if '' then 0
	# PLOT, REGISTRATION_DATE, PUBLIC_REGISTER_DATE - if '' then NULL
	'zalog': u"insert akb_zalog select {0} AGREEMEN_ID, {1} PLEDGE_ID, '{2}' REAL_ESTATE_TYPE, {3} ROOMS_COUNT, {4} TOTAL_AREA, {5} FLOORSPACE, '{6}' NUMBER_OF_STOREYS, {7} PLEDGE_AGREEMENT_VALUE, {8} PLEDGE_AGREEMENT_TOOL, {9} PLOT, '{10}' CERTIFICATE_OF_REGISTRATION, convert(smalldatetime,{11}, 103) REGISTRATION_DATE, convert(smalldatetime,{12}, 103) PUBLIC_REGISTER_DATE, '{13}' PUBLIC_REGISTER_NUMBER",
	
	# PRICE, PASSPORT_DELIVERY_DATE, RACE - if '' then NULL
	'zalog_auto': u"insert akb_zalog_auto select {0} AGREEMENT_ID,{1} NEW_FLAG,{2} PRICE, {3} ESTIMATED_VALUE,'{4}' SELLER_TYPE, '{5}' SELLER, '{6}' MOTOR_SHOW,'{7}' BRAND,'{8}' CAR_TYPE,'{9}' VIN, '{10}' BODY_NUMBER,'{11}' VEHICLE_NUMBER,'{12}' COLOR, {13} ENGINE_DISPLACEMENT,{14} ENGINE_POWER,{15} ISSUE_YEAR, '{16}' PASSPORT_SERIES,'{17}' PASSPORT_WHOM_GIVEN, convert(date,'{18}', 103) PASSPORT_GIVEN_DATE, convert(date,{19}, 103) PASSPORT_DELIVERY_DATE, {20} PASSPORT_GIVEN_FLAG,'{21}' PASSPORT_BRANCH,{22} RACE, '{23}' COUNTRY,'{24}' BRANCH_NAME",

	# PERIOD_CLOSE_DATE, PERIOD_FACT_PAYMENT_FLAG, OPEN_DATE, 
	# BEGIN_VALIDITY_DATE, CLOSE_DATE - if '' then NULL
	'insurance': u"insert  akb_insurance select {0} AGREEMENT_ID, '{1}' INSURANCE_NUM, {2} EXISTS_PLEDGE_FLAG, {3} EXISTS_TITLE_FLAG, {4} EXISTS_LIVE_FLAG, '{5}' PLEDGE_FIRM_NAME, '{6}' TITLE_FIRM_NAME, '{7}' LIVE_FIRM_NAME, {8} PLEDGE_AMOUNT, {9} TITLE_AMOUNT, {10} LIVE_AMOUNT, {11} TOOL_ISO_CODE, convert(date,{12}, 103) PERIOD_CLOSE_DATE, {13} PERIOD_FACT_PAYMENT_FLAG, '{14}' SUBSIDIARY_NAME, '{15}' CLIENT_SHORT_NAME, convert(date,{16}, 103) OPEN_DATE, {17} INSURANCE_AGREEMENT_PERIOD, convert(date,{18}, 103) BEGIN_VALIDITY_DATE, convert(date,{19}, 103) CLOSE_DATE, {20} INSURANCE_COUNT"
}

nulls_sql = ['CLOSE_DATE',
			 'BRANCH_NAME',
			 'PLOT',
			 'REGISTRATION_DATE',
			 'PUBLIC_REGISTER_DATE',
			 'PRICE',
			 'PASSPORT_DELIVERY_DATE',
			 'RACE',
			 'PERIOD_CLOSE_DATE',
			 'PERIOD_FACT_PAYMENT_FLAG',
			 'OPEN_DATE',
			 'BEGIN_VALIDITY_DATE']

zeros_sql = ['ANNUITY',
			 'OVERDUE_DAY',
			 'ROOMS_COUNT',
			 'FLOORSPACE',
			 'PLEDGE_AGREEMENT_VALUE']

titles_sql = ['FIRST_NAME',
			  'LAST_NAME',
			  'MIDDLE_NAME']

null_need_colon = ['REGISTRATION_DATE',
				   'PUBLIC_REGISTER_DATE',
				   'PASSPORT_DELIVERY_DATE',
				   'PERIOD_CLOSE_DATE',
				   'OPEN_DATE',
				   'BEGIN_VALIDITY_DATE',
				   'CLOSE_DATE']


class InsertNewAB(object):
	def __init__(self, xml_path=None):
		if xml_path != None:
			self.xml_path = xml_path
			self.read_xml(self.xml_path)
			self.get_ids()
	
	def read_xml(self, path):
		root = ElementTree.parse(path).getroot()
		self.sql_path = root.find('sql-script-destination').text.strip()
		self.was_ab_path = root.find('was-ab').text.strip()
		self.ip_m_path = root.find('ipotheka-machine').text.strip()
		self.copy_from = root.find('copy-from').text.strip()

	def generate_script(self):
		credit_agreement_ids = pd.read_csv(self.ip_m_path.strip()+credit, 
											usecols=['#AGREEMENT_ID'],
											delimiter = ';')
		new_credits = InsertNewAB.compare(credit_agreement_ids,
									self.agreement_ids, 
									'#AGREEMENT_ID',
									'AGREEMENT_ID')
		sql_script_df = pd.DataFrame(columns=sql_script_df_structure)
		if new_credits.empty:
			print "No new credits"
		else:
			print "New credits! Generating script..."
			df = pd.DataFrame(columns=sql_script_df_structure)
			df = df.append(self.generate_sql_df(address, 'address'))
			df = df.append(self.generate_sql_df(client_dog, 'client_dog', client_id=False))
			df = df.append(self.generate_sql_df(client, 'client', client_id=True))
			df = df.append(self.generate_sql_df(passport, 'passport', client_id=True))
			df = df.append(self.generate_sql_df(contact, 'contact', client_id=True))
			df = df.append(self.generate_sql_df(credit, 'credit', client_id=False))
			df = df.append(self.generate_sql_df(zalog, 'zalog', client_id=False))
			df = df.append(self.generate_sql_df(zalog_auto, 'zalog_auto', client_id=False))
			df = df.append(self.generate_sql_df(insurance, 'insurance', client_id=False))
			df.to_csv(self.sql_path, 
						header=None, 
						index=None, 
						delimiter='', 
						columns=['SQL'], 
						encoding="windows-1251")

	def generate_sql_df(self, df_file, df_name, client_id=True):
		result = pd.DataFrame(columns=sql_script_df_structure)
		df_csv_df = pd.read_csv(self.ip_m_path.strip() + df_file,
								encoding="windows-1251",
								delimiter=";")
		## Decide ID to compare DFs
		if client_id:
			df_id = '#CLIENT_ID'
			df = InsertNewAB.compare(df_csv_df, 
							   self.client_ids,
							   df_id,
							   df_id.replace('#',''))
		elif not client_id and df_file not in [client_dog, zalog_auto]:
			df_id = '#AGREEMENT_ID'
			df = InsertNewAB.compare(df_csv_df, 
							   self.agreement_ids,
							   df_id,
							   df_id.replace('#',''))
		else:
			df_id = '#AGREEMEN_ID'
			df = InsertNewAB.compare(df_csv_df, 
							   self.agreement_ids,
							   df_id,
							   'AGREEMENT_ID')
		df = df.fillna('')
		if not df.empty:
			print "Executing " + df_name + ".csv..."
			sql = []
			for index, row in df.iterrows():
				row_list = []
				for x, y in zip(row, row.index):

					if x != '':
						if y in null_need_colon:
							row_list.append("'" + x + "'")
						elif y in titles_sql:
							row_list.append(x.title())
						else:
							row_list.append(x)
					else:
						if y in nulls_sql:
							row_list.append('NULL')
						elif y in zeros_sql:
							row_list.append(0)
						else:
							row_list.append('')
				sql.append((df_name, row[df_id], inserts[df_name].format(*tuple(row_list)).strip(' \t\n\r')))
			result = result.append(pd.DataFrame(sql, 
												columns=sql_script_df_structure))
		return result.drop_duplicates()

	@staticmethod
	def compare(what, where, what_key, where_key):
		return what.loc[~what[what_key].isin(where[where_key])]

	def get_ids(self):
		self.agreement_ids = pd.read_excel(self.was_ab_path.strip(), 
											sheetname='agrrement_id',
											parse_cols = 'B')
		self.client_ids = pd.read_excel(self.was_ab_path.strip(), 
										sheetname='client_id',
										parse_cols = 'B')

	def remove_files(self):
		filelist = glob.glob(self.ip_m_path+'*.*')
		for f in filelist:
			os.remove(f)

	def copy_files(self):
		for filename in glob.glob(os.path.join(self.copy_from, '*.*')):
			shutil.copy(filename, self.ip_m_path)
		print calendar.day_name[datetime.datetime.today().weekday()] 
		if calendar.day_name[datetime.datetime.today().weekday()] == 'Monday':
			shutil.copy2(self.generate_dp_path(minus_days=1), self.ip_m_path)
			shutil.copy2(self.generate_dp_path(minus_days=2), self.ip_m_path)

	def generate_dp_path(self, minus_days):
		today_m1 = InsertNewAB.generate_date(minus_days=minus_days, last_two_digits_year=True)
		today_m3 = InsertNewAB.generate_date(minus_days=minus_days+2)
		return self.copy_from + '\\Archive\\' + today_m1[0] + today_m1[1] + today_m1[2] \
								 + '\\DAILY_PAY_' + today_m3[0] + '_' + today_m3[1] + '_' \
								 + today_m3[2] + '.csv'

	@staticmethoddef generate_date(minus_days, last_two_digits_year=False):
		today = datetime.datetime.today()
		target_date = today - datetime.timedelta(days=minus_days)
		str_year = str(target_date.year)
		if last_two_digits_year:
			str_year = str_year[2:]
		str_month = str(target_date.month)
		if target_date.month < 10:
			str_month = '0' + str_month
		str_day = str(target_date.day)
		if target_date.day < 10:
			str_day = '0' + str_day
		else:
			str_day = str(target_date.day)
		return (str_year, str_month, str_day)

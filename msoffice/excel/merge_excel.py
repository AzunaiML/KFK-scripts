import pandas as pd
import numpy as np
import glob

def merge_excel(input_path, 
				output_path, 
				sheetname=None,
				columns_list=None,
				header=0,
				xls_extentions_list=['xls'],
				file_names=False):
	"""
	merge_excel объединяет excel-файлы в 1
	input_path - путь к файлам, которые нужно соеденить в 1
	columns_list - колонки, которые нужны
	output_path - путь к выходному файлу
	header, optional - строка, с которой начинается заголовок
	xls_extentions_list, optional - list с расширениями файлов excel
	file_names, optional, flag - добавлять ли столбец с именами файлов
	"""
	output = pd.DataFrame()
	for ext in xls_extentions_list:
		for f in glob.glob(path + "*." + ext):
			df = pd.read_excel(f, index_col=None, header=header, sheetname=sheetname)
			if columns_list != None:
				df = df[columns_list]		
			if file_names == True:
				fn = glob.os.path.basename(f)
				df[u'File'] = [fn] * len(df)
			output = output.append(df, ignore_index=False)
	output.to_excel(output_path, index=False) 

import pandas as pd


def split_numbers(raw_file, column, return_file, delimiter=";"):
	df = pd.read_excel(raw_file)
	s = df[column].str.split(delimiter).apply(pd.Series, 1).stack()
	print '1'
	s.index = s.index.droplevel(-1)
	s.name = column
	del df[column]
	merged = df.join(s)
	print '2'
	merged.to_excel(return_file)

if __name__ == '__main__':
	split_numbers('tel.xlsx', 'mob_tel', 'kc.xlsx')	
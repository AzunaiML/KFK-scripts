# -*- coding: utf-8 -*-
import docx
import pandas as pd

'''
Generate .docx using template in Word and data from Excel file by key
'''
excel_file_path = ''
word_template_file_path = ''
output_file_path = ''
df = pd.read_excel(excel_file_path)
columns_names = df.columns.values.tolist()
for index, row in df.iterrows(): 
  doc = docx.Document(word_template_file_path) 
  for col in columns_names:  
    for paragraph in doc.paragraphs:   
      if col in paragraph.text:    
        paragraph.text = paragraph.text.replace(col,row[col]) 
  doc.save(output_file_path + row['file_name'] + '.docx')

# -*- coding: utf-8 -*-

from InsertNewAB import InsertNewAB

if __name__ == '__main__':
	xml_path = u'D:\InsertNewAB\paths.xml'
	inab = InsertNewAB(xml_path=xml_path)
	inab.remove_files()
	inab.copy_files()
	inab.generate_script()

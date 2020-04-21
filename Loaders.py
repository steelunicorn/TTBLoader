import csv
import logging
import re
import time
from datetime import datetime

import xlrd
from dbfread import DBF

import config

max_tries = 10
wait_time = 5


class Loaders(object):

	@staticmethod
	def gc_prodcode(code):
		code = str(code).strip()
		if code == '':
			return None
		if code.find(' ') != -1:
			return None

		if code.find('_') != -1:
			code = code[:code.find('_')]
		if code.find('.') != -1:
			code = code[:code.find('.')]

		if len(code) < 6:
			code = ('000000'+code)[-6:]
		elif len(code) > 6:
			if code[:3] == '999':
				code = code.replace('999', '')
			elif code[:2] == '92':
				code = code.replace('92', '2-')

		return code

	@staticmethod
	def is_float(string):
		try:
			float(string)
			return True
		except ValueError:
			return False

	def prices_storage_insert(self):
		query = """with prod_svss as (
	select dp.id, dp.code, svss.svss
	from db1_product dp 
	left join svss on svss.id = dp.id
	where dp.class = 14745601 and dp.type = 14745601
)
insert into prices_storage (dateprice, kodpost, product_id, price)
select distinct
	coalesce(dateprice, current_date)
	, rf.kodpost
	, p.id
	, min(t.price) price
from (
	select code, price, id1, id2, mask, dateprice from tmp_ttb where lvl = 1
	union
	select t.code, min(t.price), t.id1, t.id2, t.mask, t.dateprice 
	from tmp_ttb t 
	left join (select * from tmp_ttb tt where lvl = 1) tt on tt.code = t.code and tt.id1 = t.id1  and tt.id2 = t.id2 and tt.dateprice = t.dateprice 
	where t.lvl > 1 and tt.code is null
	group by t.code, t.id1, t.id2, t.mask, t.dateprice
) t
inner join rivalformats rf on coalesce(rtrim(ltrim(rf.mask)),'') = coalesce(rtrim(ltrim(t.mask)),'') and coalesce(rtrim(ltrim(rf.id1)),'') = coalesce(rtrim(ltrim(t.id1)),'') and coalesce(rtrim(ltrim(rf.id2)),'') = coalesce(rtrim(ltrim(t.id2)),'')
left join prod_svss p on p.code = case when length(t.code)<6 then lpad(t.code::varchar,6,'0') else t.code end
where ( p.svss=0 or p.svss is null
		or ((t.price/p.svss*100-100) between -17 and 500 and p.svss<=50)
		or ((t.price/p.svss*100-100) between -17 and 100 and p.svss>50 and p.svss<=100)
		or ((t.price/p.svss*100-100) between -17 and 100 and p.svss>100 and p.svss<=250)
		or ((t.price/p.svss*100-100) between -13 and 90 and p.svss>250 and p.svss<=500)
		or ((t.price/p.svss*100-100) between -13 and 90 and p.svss>500 and p.svss<=1000)
		or ((t.price/p.svss*100-100) between -9 and 80 and p.svss>1000 and p.svss<=2000)
		or ((t.price/p.svss*100-100) between -9 and 80 and p.svss>2000))
	and t.price>0
group by coalesce(dateprice, current_date), rf.kodpost, p.id
on conflict on constraint prices_storage_pk do update set price = least(prices_storage.price, excluded.price)"""
		try:
			return self.pgdb.query(query)
		except Exception as e:
			self.logger.error('Ошибка при записи данных в хранилище цен: {}'.format(e))
			return None

	def rivalcodes_update(self):
		query = """insert into rivalcodes (id, extcode, idrival)
select distinct p.id, t.extcode, rc.idrival
from tmp_ttb t
inner join db1_product p on p.code = t.code
inner join rivalformats rf on coalesce(rtrim(ltrim(rf.id1)),'') = coalesce(rtrim(ltrim(t.id1)),'') and coalesce(rtrim(ltrim(rf.id2)),'') = coalesce(rtrim(ltrim(t.id2)),'') and coalesce(rtrim(ltrim(rf.mask)),'') = coalesce(rtrim(ltrim(t.mask)),'')
inner join rivalconnections rc on rc.kodpost = rf.kodpost
where rtrim(ltrim(t.extcode))<>'' and t.extcode is not null
on conflict on constraint rivalcodes_pk do update set lastupd = now()"""
		try:
			return self.pgdb.query(query)
		except Exception as e:
			self.logger.error('Ошибка при обновлении привязок кодов конкурентов: {}'.format(e))
			return None

	def rivalconnections_update(self):
		query = """with updcount as (
	select rf.kodpost, count(distinct code), max(dateprice) dateprice
	from tmp_ttb t
	inner join rivalformats rf on coalesce(rtrim(ltrim(rf.mask)),'') = coalesce(rtrim(ltrim(t.mask)),'') and coalesce(rtrim(ltrim(rf.id1)),'') = coalesce(rtrim(ltrim(t.id1)),'') and coalesce(rtrim(ltrim(rf.id2)),'') = coalesce(rtrim(ltrim(t.id2)),'')
	group by rf.kodpost
	having count(distinct code)>200
)
update rivalconnections rc set lastupd = u.dateprice
from updcount u
where u.kodpost = rc.kodpost and (rc.lastupd < u.dateprice or rc.lastupd is null)"""
		try:
			return self.pgdb.query(query)
		except Exception as e:
			self.logger.error('Ошибка при записи даты обновления первоистоников: {}'.format(e))
			return None

	def loader_iacsv(self, _file, _mask):
		insert_query = "insert into tmp_ttb (code, price, id1, id2, mask, extcode, dateprice, lvl, product_id, producer_id) values %s"

		def lazy_iter():
			keep_trying = True
			tries = 1
			while keep_trying:
				try:
					with open(_file, 'r') as file:
						for row in csv.DictReader(file, delimiter=';', quoting=csv.QUOTE_ALL):
							try:
								str_level = row['LEVEL'] if 'LEVEL' in row else 1
								str_date = datetime.strptime(row['DATE'][:10], '%Y-%m-%d') if 'DATE' in row else datetime.today()
								str_price = row['PRICE'].replace(',', '.') or 0
								str_sum = (0 if not self.is_float(row['STOCK']) else float(row['STOCK']))*(0 if not self.is_float(str_price) else float(str_price))
								str_code = row['GCCODE'] if 'GCCODE' in row else row['AXCODE'] if 'AXCODE' in row else None
								str_productid = row['PRODUCT_ID'] if 'PRODUCT_ID' in row else None
								str_producerid = row['PRODUCER_ID'] if 'PRODUCER_ID' in row else None
								str_sellercode = row['SELLER_CODE'] if 'SELLER_CODE' in row else row['SUPCODE'] if 'SUPCODE' in row else None
								str_supid = row['SUP_ID'] if 'SUP_ID' in row else None
								str_priceid = row['PRICE_ID'] if 'PRICE_ID' in row else None
								if str_sum > float(config.sum_in_row) and all(x is not None for x in [str_code, str_priceid, str_supid]):
									yield [str_code, str_price, row['SUP_ID'], row['PRICE_ID'], _mask, str_sellercode, str_date, str_level, str_productid, str_producerid]
								else:
									continue

							except Exception as err:
								self.logger.error('Ошибка при обработке строки файла {}\n{}: {}'.format(_file, row, err))
								continue
						keep_trying = False
				except PermissionError:
					if tries <= max_tries:
						self.logger.info('Файл занят {}. Ожидаю {} секунд.'.format(_file, wait_time))
						tries += 1
						time.sleep(wait_time)
						continue
					else:
						self.logger.info('Файл занят слишком долго {}. Пропускаю.'.format(_file, wait_time))
						return
				except Exception as err:
					self.logger.error('Ошибка при открытии файла {}: {}'.format(_file, err))
					return
		try:
			self.pgdb.query("""insert into platformcodes (id, pid, ext_prodid , ext_mfid)
select distinct 
	p.id
	, 1 -- Аналитфармация
	, t.product_id
	, t.producer_id
from tmp_ttb t
inner join db1_product p on p.code = t.code
where t.product_id is not null and t.producer_id is not null 
on conflict on constraint platformcodes_pk do update set lastupd = now()""")
		except Exception as e:
			self.logger.error(f'Ошибка при обновлении platformcodes: {e}')
			pass

		try:
			self.pgdb.batch_insert(insert_query, lazy_iter())
		except Exception as e:
			self.logger.error('Ошибка при записи данных файла {} во временную таблицу: {}'.format(_file, e))

		return self.pgdb.query('select count(*) from tmp_ttb')[0]['count']

	def loader_iaprotek(self, _file, _mask):
		insert_query = "insert into tmp_ttb (code, price, id1, mask) values %s"
		startrow = 6

		def lazy_iter():
			keep_trying = True
			tries = 1
			while keep_trying:
				try:
					with xlrd.open_workbook(_file) as wb:
						for sh in wb.sheets():
							for row in range(startrow, sh.nrows):
								try:
									yield [sh.cell(row, 0).value, sh.cell(row, 7).value, sh.name, _mask]
								except Exception as err:
									self.logger.error('Ошибка при обработке строки файла {}: {}'.format(_file, err))
									continue
						keep_trying = False
				except PermissionError:
					if tries <= max_tries:
						self.logger.info('Файл занят {}. Ожидаю {} секунд.'.format(_file, wait_time))
						tries += 1
						time.sleep(wait_time)
						continue
					else:
						self.logger.info('Файл занят слишком долго {}. Пропускаю.'.format(_file, wait_time))
						return
				except Exception as err:
					self.logger.error('Ошибка при открытии файла {}: {}'.format(_file, err))
					return

		try:
			self.pgdb.batch_insert(insert_query, lazy_iter())
		except Exception as e:
			self.logger.error('Ошибка при записи данных файла {} во временную таблицу: {}'.format(_file, e))

		return self.pgdb.query('select count(*) from tmp_ttb')[0]['count']

	def loader_iafivemin(self, _file, _mask):
		insert_query = "insert into tmp_ttb (code, price, id1, id2, mask, dateprice) values %s"
		startrow = 7
		res = self.pgdb.query('select min(id2::int), max(id2::int) from rivalformats where lower(format) = %s and mask = %s', ['iafivemin', _mask])

		def lazy_iter():
			keep_trying = True
			tries = 1
			while keep_trying:
				try:
					with xlrd.open_workbook(_file) as wb:
						for sh in wb.sheets():
							dt = datetime.strptime(sh.cell(0, 0).value[sh.cell(0, 0).value.find('создан') + 7:], '%d.%m.%Y %H:%M:%S')
							for row in range(startrow, sh.nrows):
								for k in range(res[0]['min']-1, res[0]['max']):
									if sh.cell_type(row, k) == xlrd.XL_CELL_NUMBER:
										try:
											yield [sh.cell(row, 0).value, sh.cell(row, k).value, sh.name, str(k + 1), _mask, dt]
										except Exception as err:
											self.logger.error('Ошибка при обработке строки файла {}: {}'.format(_file, err))
											continue
						keep_trying = False
				except PermissionError:
					if tries <= max_tries:
						self.logger.info('Файл занят {}. Ожидаю {} секунд.'.format(_file, wait_time))
						tries += 1
						time.sleep(wait_time)
						continue
					else:
						self.logger.info('Файл занят слишком долго {}. Пропускаю.'.format(_file, wait_time))
						return
				except Exception as err:
					self.logger.error('Ошибка при открытии файла {}: {}'.format(_file, err))
					return

		try:
			self.pgdb.batch_insert(insert_query, lazy_iter())
		except Exception as e:
			self.logger.error('Ошибка при записи данных файла {} во временную таблицу: {}'.format(_file, e))

		return self.pgdb.query('select count(*) from tmp_ttb')[0]['count']

	def loader_iametr(self, _file, _mask):
		insert_query = "insert into tmp_ttb (code, price, id1, id2, mask, dateprice) values %s"
		startrow = 3
		startcol = 11

		def lazy_iter():
			keep_trying = True
			tries = 1
			while keep_trying:
				try:
					with xlrd.open_workbook(_file) as wb:
						for sh in wb.sheets():
							for row in range(startrow, sh.nrows):
								for col in range(startcol, sh.ncols, 2):
									if sh.cell_type(row, col) == xlrd.XL_CELL_NUMBER and sh.cell_type(row, 0) == xlrd.XL_CELL_TEXT:
										try:
											yield [sh.cell(row, 0).value, sh.cell(row, col).value, sh.cell(0, col).value, sh.name, _mask, datetime.strptime(sh.cell(1, col).value, '%d.%m.%Y %H:%M:%S')]
										except Exception as err:
											self.logger.error('Ошибка при обработке строки файла {}: {}'.format(_file, err))
											continue
						keep_trying = False
				except PermissionError:
					if tries <= max_tries:
						self.logger.info('Файл занят {}. Ожидаю {} секунд.'.format(_file, wait_time))
						tries += 1
						time.sleep(wait_time)
						continue
					else:
						self.logger.info('Файл занят слишком долго {}. Пропускаю.'.format(_file, wait_time))
						return
				except Exception as err:
					self.logger.error('Ошибка при открытии файла {}: {}'.format(_file, err))
					return

		try:
			self.pgdb.batch_insert(insert_query, lazy_iter())
		except Exception as e:
			self.logger.error('Ошибка при записи данных файла {} во временную таблицу: {}'.format(_file, e))

		return self.pgdb.query('select count(*) from tmp_ttb')[0]['count']

	def loader_fefivemin(self, _file, _mask):
		insert_query = """with tmp_fe as (
	select vls.column1 as name, vls.column2 as code, vls.column3 as mf, vls.column4 as cost, vls.column5 as id1, vls.column6 as id2, vls.column7 as mask, vls.column8 as dateprice  
	from (values %s) as vls
) 
, inserter as (
	insert into fe_product (id, fe_name, fe_mf) 
	select distinct p.id, tmp_fe.name, tmp_fe.mf 
	from tmp_fe 
	inner join db1_product p on p.code = tmp_fe.code where tmp_fe.code<>'' on conflict on constraint fe_product_pk do nothing
)
insert into tmp_ttb (code, price, id1, id2, mask, dateprice)
select 
	pr.code
	, tmp_fe.cost
	, tmp_fe.id1
	, tmp_fe.id2
	, tmp_fe.mask
	, tmp_fe.dateprice
from tmp_fe
inner join fe_product p on p.fe_name = tmp_fe.name and p.fe_mf = tmp_fe.mf 
inner join db1_product pr on pr.id = p.id"""
		startrow = 5

		def lazy_iter():
			keep_trying = True
			tries = 1
			while keep_trying:
				try:
					with xlrd.open_workbook(_file) as wb:
						for sh in wb.sheets():
							id1 = sh.cell(3, 0).value.split(':')[1].strip()
							dt = datetime.strptime(sh.cell(0, 0).value.split(' ')[2], '%d.%m.%Y')
							for row in range(startrow, sh.nrows):
								for col in range(0, sh.ncols):
									if sh.cell(startrow - 1, col).value.find('Мин цена') != -1 and sh.cell_type(row, col) == xlrd.XL_CELL_NUMBER:
										if sh.cell_type(row, 1) == xlrd.XL_CELL_NUMBER:
											code = ('000000' + str(int(sh.cell(row, 1).value)))[-6:]
										else:
											code = sh.cell(row, 1).value.strip()
										for cd in code.split(','):
											yield [sh.cell(row, 0).value.strip(), cd.strip(), sh.cell(row, 2).value.strip(), sh.cell(row, col).value, id1, sh.cell(row, col + 1).value.strip(), _mask, dt]
						keep_trying = False
				except PermissionError:
					if tries <= max_tries:
						self.logger.info('Файл занят {}. Ожидаю {} секунд.'.format(_file, wait_time))
						tries += 1
						time.sleep(wait_time)
						continue
					else:
						self.logger.info('Файл занят слишком долго {}. Пропускаю.'.format(_file, wait_time))
						return
				except Exception as err:
					self.logger.error('Ошибка при открытии файла {}: {}'.format(_file, err))
					return

		try:
			self.pgdb.batch_insert(insert_query, lazy_iter())
		except Exception as e:
			self.logger.error('Ошибка при записи данных файла {} во временную таблицу: {}'.format(_file, e))

		return self.pgdb.query('select count(*) from tmp_ttb')[0]['count']

	def loader_eprica(self, _file, _mask):
		insert_query = """insert into tmp_ttb (code, price, mask)
select p.code, e.column2::numeric as cost, e.column3 as mask
from (values %s) e
inner join rivalformats rf on rf.mask = e.column3
inner join rivalconnections rconn on rconn.kodpost = rf.kodpost
inner join rivalcodes rc on rc.extcode = e.column1 and rc.idrival = rconn.idrival
inner join db1_product p on p.id = rc.id"""

		def lazy_iter():
			keep_trying = True
			tries = 1
			while keep_trying:
				try:
					with open(_file, 'r') as file:
						for row in csv.reader(file, delimiter=';', quoting=csv.QUOTE_NONE):
							try:
								yield [row[0], float(row[1].replace(',', '.')), _mask]
							except Exception as err:
								self.logger.error('Ошибка при обработке строки файла {}: {}'.format(_file, err))
								continue
						keep_trying = False
				except PermissionError:
					if tries <= max_tries:
						self.logger.info('Файл занят {}. Ожидаю {} секунд.'.format(_file, wait_time))
						tries += 1
						time.sleep(wait_time)
						continue
					else:
						self.logger.info('Файл занят слишком долго {}. Пропускаю.'.format(_file, wait_time))
						return
				except Exception as err:
					self.logger.error('Ошибка при открытии файла {}: {}'.format(_file, err))
					return

		try:
			self.pgdb.batch_insert(insert_query, lazy_iter())
		except Exception as e:
			self.logger.error('Ошибка при записи данных файла {} во временную таблицу: {}'.format(_file, e))

		return self.pgdb.query('select count(*) from tmp_ttb')[0]['count']

	def loader_yugfarm(self, _file, _mask):
		insert_query = """insert into tmp_ttb (code, price, mask)
select p.code, e.column2::numeric as cost, e.column3 as mask
from (values %s) e
inner join rivalformats rf on rf.mask = e.column3
inner join rivalconnections rconn on rconn.kodpost = rf.kodpost
inner join rivalcodes rc on rc.extcode = e.column1 and rc.idrival = rconn.idrival
inner join db1_product p on p.id = rc.id"""
		startrow = 4

		def lazy_iter():
			keep_trying = True
			tries = 1
			while keep_trying:
				try:
					with xlrd.open_workbook(_file) as wb:
						for sh in wb.sheets():
							for row in range(startrow, sh.nrows):
								try:
									yield [sh.cell(row, 6).value, sh.cell(row, 3).value, _mask]
								except Exception as err:
									self.logger.error('Ошибка при обработке строки файла {}: {}'.format(_file, err))
									continue
						keep_trying = False
				except PermissionError:
					if tries <= max_tries:
						self.logger.info('Файл занят {}. Ожидаю {} секунд.'.format(_file, wait_time))
						tries += 1
						time.sleep(wait_time)
						continue
					else:
						self.logger.info('Файл занят слишком долго {}. Пропускаю.'.format(_file, wait_time))
						return
				except Exception as err:
					self.logger.error('Ошибка при открытии файла {}: {}'.format(_file, err))
					return

		try:
			self.pgdb.batch_insert(insert_query, lazy_iter())
		except Exception as e:
			self.logger.error('Ошибка при записи данных файла {} во временную таблицу: {}'.format(_file, e))

		return self.pgdb.query('select count(*) from tmp_ttb')[0]['count']

	def loader_april(self, _file, _mask):
		insert_query = """insert into tmp_ttb (code, price, id1, mask)
select 
	pr.code
	, vls.column2
	, vls.column3
	, vls.column4
from (values %s) vls
inner join db1_product ean on ean.code = vls.column1 and ean.class = 14745603 and ean.type = 14745604
inner join db1_product pr on pr.id = ean.pid"""
		startrow = 1

		def lazy_iter():
			keep_trying = True
			tries = 1
			while keep_trying:
				try:
					with xlrd.open_workbook(_file) as wb:
						for sh in wb.sheets():
							for row in range(startrow, sh.nrows):
								yield [sh.cell(row, 5).value.strip(), sh.cell(row, 2).value, sh.cell(row, 4).value.strip(), _mask]
						keep_trying = False
				except PermissionError:
					if tries <= max_tries:
						self.logger.info('Файл занят {}. Ожидаю {} секунд.'.format(_file, wait_time))
						tries += 1
						time.sleep(wait_time)
						continue
					else:
						self.logger.info('Файл занят слишком долго {}. Пропускаю.'.format(_file, wait_time))
						return
				except Exception as err:
					self.logger.error('Ошибка при открытии файла {}: {}'.format(_file, err))
					return

		try:
			self.pgdb.batch_insert(insert_query, lazy_iter())
		except Exception as e:
			self.logger.error('Ошибка при записи данных файла {} во временную таблицу: {}'.format(_file, e))

		return self.pgdb.query('select count(*) from tmp_ttb')[0]['count']

	def loader_farmnet(self, _file, _mask):
		insert_query = """with tmp_farmnet as (
	select vls.column1 as ext_code, vls.column2 as code, vls.column3 as cost, vls.column4 as id1, vls.column5 as mask  
	from (values %s) as vls
)
, platform_update as (
	insert into platformcodes (id, pid, ext_prodid, ext_mfid)
	select distinct p.id, (select id from platforms where lower(name) = 'фармнет'), ext_code, '' 
	from tmp_farmnet f
	inner join db1_product p on p.code = f.code
	where ext_code is not null and ext_code<>''
	on conflict on constraint platformcodes_pk do nothing 
)
insert into tmp_ttb (code, price, id1, mask)
select p.code, f.cost, f.id1, f.mask
from tmp_farmnet f
inner join platformcodes pc on pc.ext_prodid = f.ext_code and pc.pid = (select id from platforms where lower(name) = 'фармнет')
inner join db1_product p on p.id = pc.id
where f.cost>0"""
		startrow = 1
		startcol = 4

		def lazy_iter():
			keep_trying = True
			tries = 1
			while keep_trying:
				try:
					with xlrd.open_workbook(_file) as wb:
						for sh in wb.sheets():
							for row in range(startrow, sh.nrows):
								for col in range(startcol, sh.ncols):
									if sh.cell_type(row, col) == xlrd.XL_CELL_NUMBER:
										yield [str(int(sh.cell(row, 0).value)), self.gc_prodcode(sh.cell(row, 1).value), sh.cell(row, col).value, sh.cell(0, col).value, _mask]
									else:
										pass
						keep_trying = False
				except PermissionError:
					if tries <= max_tries:
						self.logger.info('Файл занят {}. Ожидаю {} секунд.'.format(_file, wait_time))
						tries += 1
						time.sleep(wait_time)
						continue
					else:
						self.logger.info('Файл занят слишком долго {}. Пропускаю.'.format(_file, wait_time))
						return
				except Exception as err:
					self.logger.error('Ошибка при открытии файла {}: {}'.format(_file, err))
					return

		try:
			self.pgdb.batch_insert(insert_query, lazy_iter())
		except Exception as e:
			self.logger.error('Ошибка при записи данных файла {} во временную таблицу: {}'.format(_file, e))

		return self.pgdb.query('select count(*) from tmp_ttb')[0]['count']

	def loader_pharmmarket(self, _file, _mask):
		insert_query = """insert into tmp_ttb (code, price, id1, mask, dateprice) values %s"""
		startrow = 6
		startcol = 6

		def lazy_iter():
			keep_trying = True
			tries = 1
			while keep_trying:
				try:
					with xlrd.open_workbook(_file) as wb:
						for sh in wb.sheets():
							for row in range(startrow, sh.nrows):
								for col in range(startcol, sh.ncols):
									if sh.cell_type(row, col) == xlrd.XL_CELL_NUMBER and sh.cell_type(row, 2) != xlrd.XL_CELL_EMPTY:
										yield [self.gc_prodcode(sh.cell(row, 2).value), sh.cell(row, col).value, sh.cell(3, col).value, _mask, datetime.strptime(sh.cell(4, col).value, '%d.%m.%Y')]
									else:
										pass
						keep_trying = False
				except PermissionError:
					if tries <= max_tries:
						self.logger.info('Файл занят {}. Ожидаю {} секунд.'.format(_file, wait_time))
						tries += 1
						time.sleep(wait_time)
						continue
					else:
						self.logger.info('Файл занят слишком долго {}. Пропускаю.'.format(_file, wait_time))
						return
				except Exception as err:
					self.logger.error('Ошибка при открытии файла {}: {}'.format(_file, err))
					return

		try:
			self.pgdb.batch_insert(insert_query, lazy_iter())
		except Exception as e:
			self.logger.error('Ошибка при записи данных файла {} во временную таблицу: {}'.format(_file, e))

		return self.pgdb.query('select count(*) from tmp_ttb')[0]['count']

	def loader_top1000(self, _file, _mask):
		insert_query = """insert into tmp_ttb (code, price, id1, mask, dateprice) values %s"""
		res = self.pgdb.query('select min(id1::int), max(id1::int) from rivalformats where lower(format) = %s and mask = %s', ['top1000', _mask])

		startrow = 7
		startcol = res[0]['min']-1

		def lazy_iter():
			keep_trying = True
			tries = 1
			while keep_trying:
				try:
					with xlrd.open_workbook(_file, ignore_workbook_corruption=True) as wb:
						for sh in wb.sheets():
							dt = xlrd.xldate.xldate_as_datetime(sh.cell(3, 2).value, wb.datemode)
							for row in range(startrow, sh.nrows):
								for col in range(startcol, res[0]['max']):
									if sh.cell_type(row, col) == xlrd.XL_CELL_NUMBER and sh.cell_type(row, 14) != xlrd.XL_CELL_EMPTY:
										for code in str(sh.cell(row, 14).value).split(';'):
											yield [self.gc_prodcode(code), sh.cell(row, col).value, col+1, _mask, dt]
									else:
										pass
						keep_trying = False
				except PermissionError:
					if tries <= max_tries:
						self.logger.info('Файл занят {}. Ожидаю {} секунд.'.format(_file, wait_time))
						tries += 1
						time.sleep(wait_time)
						continue
					else:
						self.logger.info('Файл занят слишком долго {}. Пропускаю.'.format(_file, wait_time))
						return
				except Exception as err:
					self.logger.error('Ошибка при открытии файла {}: {}'.format(_file, err))
					return

		try:
			self.pgdb.batch_insert(insert_query, lazy_iter())
		except Exception as e:
			self.logger.error('Ошибка при записи данных файла {} во временную таблицу: {}'.format(_file, e))

		return self.pgdb.query('select count(*) from tmp_ttb')[0]['count']

	def loader_sklit_client(self, _file, _mask):
		insert_query = """with rnk as (
	select id_name, id_mak, price, id_p, kod, zakaz_min, row_number() over (partition by id_p, id_name, id_mak, kod order by zakaz_min, price) rnk from tmp_sklit
)
, platformcodes_upd as ( -- обновляем привязки кодов площадки 
	insert into platformcodes (id, pid, ext_prodid , ext_mfid)
	select p.id, (select id from platforms where lower(name) = 'склит'), t.id_name, t.id_mak
	from rnk t
	inner join db1_product p on p.code = t.kod
	where t.id_p = %(gc_code)s and t.rnk = 1
	on conflict on constraint platformcodes_pk do update set lastupd = now()
)
, ranked as (
	select t.code, t.price, t.id_p, t.kod, row_number() over (partition by id_p, code, kod order by zakaz_min, price) rnk 
	from (
		select p.code, t.price, t.id_p, t.kod, t.zakaz_min
		from tmp_sklit t
		inner join platformcodes pc on pc.ext_prodid = t.id_name::text and pc.ext_mfid = t.id_mak::text and pc.pid = (select id from platforms where lower(name) = 'склит')
		inner join db1_product p on p.id = pc.id 
		where t.price>0 and t.id_p<>%(gc_code)s
		union
		select p.code, t.price, t.id_p, t.kod, t.zakaz_min
		from tmp_sklit t
		inner join rivalformats rf on rf.id1 = t.id_p::text and rf.mask = %(mask)s
		inner join rivalconnections rc on rc.kodpost = rf.kodpost  
		inner join rivalcodes r on r.extcode = t.kod and r.idrival = rc.idrival
		inner join db1_product p on p.id = r.id
		where t.price>0 and t.id_p<>%(gc_code)s
	) t
)
insert into tmp_ttb (code, price, id1, mask, extcode)
select code, price, id_p, %(mask)s, kod from ranked where rnk=1"""

		# в этой загрузке алгоритм чуть хитрее, и чтобы не читать файл 2 раза используем временную таблицу
		self.pgdb.query('create temp table if not exists tmp_sklit(id_name int, id_mak int, price numeric(19,2), id_p int, kod text, zakaz_min int)')
		self.pgdb.query('truncate table tmp_sklit')
		sklt_insert = "insert into tmp_sklit (values %s)"

		ids = [int(x['id_p']) for x in self.pgdb.query("select distinct id1 id_p from rivalformats where format = 'sklit_client'")]
		if config.gc_sklitcode is not None:
			ids.append(int(config.gc_sklitcode))
		else:
			pass

		def lazy_iter():
			keep_trying = True
			tries = 1
			while keep_trying:
				try:
					with DBF(_file, ignore_missing_memofile=True) as d:
						for row in d:
							if row['ID_P'] in ids and not any([re.match(x, row['CNOTE'], re.IGNORECASE) for x in ('уценка', 'срок', 'возврат')]):
								yield [row['ID_NAME'], row['ID_MAK'], row['PRICE0'], row['ID_P'], row['KOD'], row['ZAKAZ_MIN']]
							else:
								pass
						keep_trying = False
				except PermissionError:
					if tries <= max_tries:
						self.logger.info('Файл занят {}. Ожидаю {} секунд.'.format(_file, wait_time))
						tries += 1
						time.sleep(wait_time)
						continue
					else:
						self.logger.info('Файл занят слишком долго {}. Пропускаю.'.format(_file, wait_time))
						return
				except Exception as err:
					self.logger.error('Ошибка при открытии файла {}: {}'.format(_file, err))
					return

		try:
			self.pgdb.batch_insert(sklt_insert, lazy_iter())
		except Exception as e:
			self.logger.error('Ошибка при записи данных файла {} в промежуточную таблицу (tmp_sklit): {}'.format(_file, e))

		try:
			self.pgdb.query(insert_query, {'mask': _mask, 'gc_code': config.gc_sklitcode})
		except Exception as e:
			self.logger.error('Ошибка при записи данных файла {} во временную таблицу: {}'.format(_file, e))

		return self.pgdb.query('select count(*) from tmp_ttb')[0]['count']

	def __init__(self, _pgdb):
		self.logger = logging.getLogger(config.APP_NAME)
		self.pgdb = _pgdb
		self.selector = {
			'iacsv': self.loader_iacsv,
			'iaprotek': self.loader_iaprotek,
			'iafivemin': self.loader_iafivemin,
			'iametr': self.loader_iametr,
			'fefivemin': self.loader_fefivemin,
			'eprica': self.loader_eprica,
			'yugfarm': self.loader_yugfarm,
			'april': self.loader_april,
			'farmnet': self.loader_farmnet,
			'pharmmarket': self.loader_pharmmarket,
			'top1000': self.loader_top1000,
			'sklit_client': self.loader_sklit_client,
		}

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		pass

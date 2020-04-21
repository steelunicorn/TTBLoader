import os
import configparser
import logging
import sys
import fnmatch
import datetime
from itertools import chain

from postgres import Postgres
from Loaders import Loaders
import config as conf

# Возможность подключать внешние .py модули для обработки файлов. Делать так я конечно не буду, но вариант вообще интересный
# spec = importlib.util.spec_from_file_location('loaders', loaders_dir + os.sep + 'loader_iafivemin.py')
# module = importlib.util.module_from_spec(spec)
# spec.loader.exec_module(module)
# res = module.load('Rep2053_20190125_14_14.xls')


def check_folder_exists(p):
	os.makedirs(p, exist_ok=True)
	return os.path.exists(p)


def get_mask_list(_pgdb):
	return _pgdb.query('select distinct mask, format from rivalformats')


def check_folder(workdir, masks):
	try:
		files = list(chain.from_iterable(filter(lambda y: len(y) > 0, map(lambda x: [{'file': i, 'mask': x['mask'], 'format': x['format']} for i in os.listdir(workdir) if os.path.isfile(workdir+os.sep+i) and fnmatch.fnmatch(i, ('' if '*' in x['mask'] else '*')+x['mask']+('' if '*' in x['mask'] else '*'))], masks))))
	except FileNotFoundError:
		logger.error('Нет каталога указанного в настройках {}'.format(workdir))
		files = []

	return files


def move_parsed(workdir, source_file):
	archive_dir = workdir+os.sep+'parsed'+os.sep+datetime.datetime.now().strftime('%Y%m%d')
	dest_file = source_file[:source_file.rindex('.')]+'_'+datetime.datetime.now().strftime('%Y%m%d_%H%M%S')+source_file[source_file.rindex('.'):]
	if check_folder_exists(archive_dir):
		try:
			os.rename(workdir+os.sep+source_file, archive_dir+os.sep+dest_file)
		except Exception as e:
			logger.error("Ошибка перемещения файла {} в архив. {}".format(source_file, e))


def tmp_ttb_create(_pgdb):
	q = 'create temp table tmp_ttb (code text, price numeric(18,2), id1 text, id2 text, mask text, extcode text, dateprice date not null default current_date, lvl int not null default 1, product_id text, producer_id text)'
	try:
		_pgdb.query(q)
	except Exception as e:
		logger.error('Произошла ошибка при создании временной таблицы {}'.format(e.args[0]))


def main():
	logger.info('Начало работы')
	for s in cfg.sections():
		updflag = False
		logger.name = conf.APP_NAME + '.' + s

		try:
			conf.gc_sklitcode = cfg.getint(s, 'gc_sklitcode')
		except configparser.NoOptionError:
			conf.gc_sklitcode = None
			logger.info('Не указан код СКЛИТ в секции {}'.format(s))
			pass

		try:
			host = cfg.get(s, 'host')
			user = cfg.get(s, 'localdb')
			workdir = cfg.get(s, 'workdir')
			conf.sum_in_row = cfg.get(s, 'row_sum_threshold', fallback=200)
			with Postgres(host, user, conf.APP_NAME) as pgdb:
				ld = Loaders(pgdb)
				tmp_ttb_create(pgdb)
				summary = 0
				for f in check_folder(workdir, get_mask_list(pgdb)):
					filename = workdir + os.sep + f['file']
					try:
						count = ld.selector[f['format'].lower()](filename, f['mask'])
						if count-summary > 0:
							logger.info('Обработан файл {} строк: {}.'.format(f['file'], (count - summary)))
							summary = count
						if count > 0:
							updflag = True
						# даже если файл был пустой и из него ничего не записалось в базу - все равно перемещаем его чтоб не мешался
						move_parsed(workdir, f['file'])

					except Exception as e:
						logger.error('Произошла ошибка при попытке обработать файл {}: {}'.format(f['file'], e))
						continue

				if updflag:
					logger.info('Обновлено {} привязок к кодам конкурентов.'.format(ld.rivalcodes_update()))
					logger.info('Записано {} строк в хранилище цен.'.format(ld.prices_storage_insert()))
					ld.rivalconnections_update()

		except configparser.NoOptionError as e:
			logger.warning('Нет параметра {} в секции {}'.format(e.option, e.section))
			continue
		except Exception as e:
			logger.error('Произошла ошибка {}'.format(e))
			continue


if __name__ == '__main__':
	if getattr(sys, 'frozen', False):
		cwd = os.path.dirname(sys.executable)
	else:
		cwd = os.path.dirname(__file__)

	settings = os.path.join(cwd, 'config.ini')

	logfile = os.path.join(cwd, 'logs', f'{datetime.datetime.now():%Y%m%d}.log')
	log_format = logging.Formatter(fmt='%(asctime)s [%(name)s][%(levelname)s]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
	handlers = [logging.StreamHandler(sys.stdout)]

	if check_folder_exists(os.path.join(cwd, 'logs')):
		handlers.append(logging.FileHandler(logfile, encoding='utf-8'))

	logger = logging.getLogger(conf.APP_NAME)
	logger.setLevel(logging.INFO)

	for h in handlers:
		h.setFormatter(log_format)
		h.setLevel(logging.INFO)
		logger.addHandler(h)

	try:
		cfg = configparser.ConfigParser(inline_comment_prefixes=';')
		cfg.read_file(open(settings, 'r'))
		main()
		logger.name = conf.APP_NAME
		logger.info('Работа завершена')
	except FileNotFoundError as err:
		print('Не найден файл config.ini')
		sys.exit(1)
	except configparser.ParsingError as err:
		print(f'Ошибка обработки config.ini:\n{err.message}')
		sys.exit(1)

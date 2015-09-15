#!/usr/bin/python
"""
	Database Pull Down Tool
	requires Mysql Client
	Python Mysql module
	Make sure to update the configs in this file, 
"""

import sys
import os
import shutil
import subprocess
import datetime
from argparse import ArgumentParser
from classes.DriverMysql import DriverMysql
from classes.BoojPushBullet import BoojPushBullet
from configs.databases import databases
from configs.general import general

##### CONFIGS #######

default_source_db         = general['default_source_db']
default_dest_db           = general['default_dest_db']
pulldown_location         = general['pulldown_location']
no_write_databases        = general['no_write_databases']
use_push_bullet           = general['use_push_bullet']
use_push_bullet_key       = general['use_push_bullet_key']
alloted_backup_size       = general['alloted_backup_size']       # Size in Gigs to keep the pull down location under


##### END CONFIGS ######

script_start_time  = datetime.datetime.now()

class PullDown( object ):

	def __init__( self, args ):
		self.args             = args
		self.source_db        = self.__set_db( 'source', self.args.sourceDB )
		self.dest_db          = self.__set_db( 'dest', self.args.destDB )		
		self.download_size    = None
		self.downloads        = {}
		self.downloaded_files = []	
		if self.args.debug:
			print self.args
			print self.source_db
			print ' '

	def run( self ):
		if self.args.database:
			self.setup_download_specific_tables()
		elif self.args.tables and '.' in self.args.tables:
			self.setup_download_specific_tables()
		self.verify_download( 'source' )
		self.download_dir     = self.__set_download_dir()
		print ' '
		print 'Starting Download from %s' % self.source_db['info']
		self.download_data( 'source' )
		if self.dest_db  and self.args.no_backup == False:
			print ' '
			print 'Starting Backup of %s' % self.dest_db['info']
			self.download_data( 'dest' )
		if self.dest_db:
			self.import_data()

		self.write_download_manifest()
		self.zip_files()
		self.cleanup()
		self.notify()

		print ''
		print 'Finished Download, Backup, Import, and Archiving in : ', str( datetime.datetime.now() - script_start_time )

	"""
		Different Download Type Section
	"""
	def setup_download_specific_tables( self ):
		if not self.args.database and '.' in self.args.tables:
			if ' ' in self.args.tables:
				unfiltered_tables = self.args.tables.split(' ')
			elif ',' in self.args.tables:
				unfiltered_tables = self.args.tables.split(',')
			else:
				unfiltered_tables = [ self.args.tables ]
			for t in unfiltered_tables:
				db = t.split('.')[0]
				tb = t.split('.')[1]
				if db in self.downloads:
					self.downloads[db]['tables'].append( tb )
				else:
					self.downloads.update( { db : {'tables' : [ tb ] } } )
			return
		else:
			database = self.args.database
			if ',' in database:
				_databases = database.split(',')
			else:
				_databases = [ self.args.database ]

			for database in _databases:
				if self.args.tables:
					tables = self.args.tables
					if ',' in tables:
						tables = tables.replace( ' ', '' ).split( ',' )
					elif ' ' in self.args.tables:
						tables = tables.split(' ')
					else:
						tables = [ tables ]
				else:
					qry = """SHOW TABLES IN %s;""" % database
					db  = DriverMysql( self.source_db )
					print qry
					result = db.execute( qry )
					tables = []
					for table in result:
						if table[0][-5:] != '_view':
							tables.append( table[0] ) 
				if self.args.verbosity:
					print 'Downloading from schema "%s" tables %s' % ( database, ','.join( tables ) )
				self.downloads.update( { database : { 'tables' : tables } } )

	def verify_download( self, source_or_dest ):
		if source_or_dest == 'source':
			db_srv = self.source_db
		else:
			db_srv = self.dest_db
		db_x = DriverMysql( db_srv )

		print 'Source Database:      ', self.source_db['info']
		print 'Destination Database: ', self.dest_db['info']
		print ' '
		self.get_slave_status()
		print 'Downloads '
		for database, info in self.downloads.iteritems():
			qry = "SHOW TABLES IN `%s`; " % database
			tables = db_x.execute( qry )

			full_table_list = []
			for table in tables:
				full_table_list.append( table[0] )
			if len( info['tables'] ) == 0:
				download_tables = full_table_list
			else:
				download_tables = info['tables']

			for dl_table in download_tables:
				if dl_table not in full_table_list:
					print 'ERROR: %s is missing from the source database!' % dl_table
					sys.exit()
			print database
			for table in info['tables']:
				print '    ', table

		if self.args.skip_verify == False:
			if self.args.structure == False:
				self.download_size = self.get_download_size( 'source' )
				print  ' '
				print 'Download Size: %s Gigs' % self.download_size['gigs']
				print 'Download Rows: %s ' % format( self.download_size['rows'], ",d")			
			print 'Continue?'
			print ' '
			verification = raw_input('--> ')
			if verification not in ( 'y', 'yes'):
				print 'Exiting'
				sys.exit()

	def get_slave_status( self ):
		if self.args.debug:
			print 'DB2 Slave Status x'
			print 'DB3 Slave Status x'
			print ' '

	def get_download_size( self, source_or_dest ):
		if source_or_dest == 'source':
			db_srv = self.source_db
		else:
			db_srv = self.dest_db

		db_x = DriverMysql( db_srv )
		totals = 0
		for database, info in self.downloads.iteritems():
			table_sql = ''
			for t in info['tables']:
				table_sql += '"%s",' % t
			table_sql = table_sql[:-1]
			if table_sql != '':
				qry = """SELECT table_name AS "Table", 
					( data_length + index_length ) as "bytes" 
					FROM information_schema.TABLES 
					WHERE table_schema = "%s"
					 AND table_name IN( %s );""" %( database, table_sql )
				for size in db_x.execute( qry, True ):
					totals = totals + int( size['bytes'] )
		megs = float( totals / 1024 ) / 1024
		gigs = megs / 1024
		gigs = round( gigs, 3 )
		if self.args.verbosity:
			print 'Download Size %s Gigs' % str( gigs )

		rows = 0
		for database, info in self.downloads.iteritems():
			for table in info['tables']:
				qry = """SELECT count(*) AS c FROM `%s`.`%s`;""" % ( database, table )
				count = db_x.execute( qry, True )[0]['c']
				rows = rows + count
		data = {
			'gigs' : gigs,
			'rows' : rows
		}
		return data

	def download_data( self, source_or_dest ):
		if source_or_dest == 'source':
			db_srv = self.source_db
			prefix = 'source'			
		elif source_or_dest =='dest':
			db_srv = self.dest_db
			prefix = 'dest'
		else:
			print "Error"
			sys.exit()

		for database, info in self.downloads.iteritems():
			pulldown_start_time = datetime.datetime.now()
			if self.args.verbosity:
				print 'Downloading data from ', database

			trickle = 'trickle -s -d1000 '
			structure = ''			
			if self.args.structure:
				structure = '-d'

			phile_name = os.path.join( self.download_dir, '%s.%s_%s.sql' % ( prefix, database, '_'.join( info['tables'] )[:30] )   )
			cmd_args   = {
				'trickle'    : trickle,
				'host'       : db_srv['host'],
				'user'       : db_srv['user'],
				'pass'       : db_srv['pass'],
				'dbname'     : database,
				'tables'     : ' '.join( info['tables'] ),
				'structure'  :  structure,
				'phile_name' : phile_name
			}

			SqlDumpCmd = '%(trickle)smysqldump -h%(host)s -u%(user)s -p%(pass)s -v --routines %(dbname)s %(tables)s %(structure)s > %(phile_name)s' % cmd_args
			if self.args.debug:
				print SqlDumpCmd
			subprocess.call( SqlDumpCmd, shell = True )

			# Add Extra info to the downloaded files.
			if self.args.verbosity:
				print '-- Wrote', cmd_args['phile_name']
			append_lines = [
				"-- Booj Data Download \n",
				"-- File Created on  %s\n" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
				"-- File Took %s to create\n" % str( datetime.datetime.now() - pulldown_start_time ),
				"-- File Includes: %s\n\n\n" % cmd_args['tables'],
			]
			phile = open( phile_name, "r+" )
			phile_lines = phile.readlines()
			phile.close()
			save_phile = []
			for line in append_lines:
				save_phile.append( line )
			for line in phile_lines:
				save_phile.append( line )
			os.remove( phile_name )
			phile = open( phile_name, "wb" )
			for x in save_phile:
				phile.write( x )
			phile.close()
			phile_info = { 
				'phile_name'     : phile_name,
				'database'       : database,
				'tables'         : cmd_args['tables'],
				'source_dl_time' : datetime.datetime.now() - pulldown_start_time,
				'type'           : source_or_dest
			}
			if self.args.verbosity:
				print ' '
			self.downloaded_files.append( phile_info )

	def import_data( self ):
		print ''
		print 'Importing Data to ', self.dest_db['info']
		db_srv = self.dest_db
		for phile in self.downloaded_files:
			if phile['type'] == 'source':
				print '-- Introducing %s to %s' % ( phile['tables'], phile['database'] )
				cmd_args   = {
					'host'       : db_srv['host'],
					'user'       : db_srv['user'],
					'pass'       : db_srv['pass'],
					'dbname'     : phile['database'],
					'phile_name' : phile['phile_name'],
				}
				cmd = "mysql -h%(host)s -u%(user)s -p%(pass)s %(dbname)s < %(phile_name)s " % cmd_args
				try:
					subprocess.call( cmd, shell = True )
				except Exception:
					print Exception
					sys.exit()
		print '-- Succesffuly imported data to ', db_srv['info']

	def write_download_manifest( self ):
		print self.download_dir

	def zip_files( self ):
		print ' '
		print 'Zipping Files '
		cmd =  'tar -cvf %s.tar -C %s .' % ( pulldown_location + '/' + self.dl_package, self.download_dir )
		# subprocess.call( cmd, shell = True )
		# shutil.rmtree( self.download_dir )

	def cleanup( self ):
		"""
			Remove files if we're over our allotted limit
		"""		
		run_cleanup = False

		backup_size = self.__get_size( pulldown_location )
		modified_backup_size = backup_size
		if run_cleanup:
			philes = os.listdir( pulldown_location )
			philes.sort()
			print ' '
			print 'Running Cleanup'
			print '  Using %s of %s GIGs of backup space' % ( str( backup_size ), str( alloted_backup_size ) )
			if delete_backups_x_days_old and ( backup_size > alloted_backup_size ):
				marked_for_deletion = []
				print '  %s Backup Files' % str( len( philes ) )
				for phile in philes:
					phile_date = phile[:19]
					try:
						phile_date = datetime.datetime.strptime( phile_date, '%Y_%m_%d_%H_%M_%S' )
						phile_age  = datetime.datetime.now() - phile_date
					except ValueError:
						continue
					if phile_age > datetime.timedelta( days = delete_backups_x_days_old ):
						marked_for_deletion.append( phile )
					else:
						size_of_backup =  self.__get_size( pulldown_location + '/' + phile )
						if modified_backup_size > alloted_backup_size:
							marked_for_deletion.append( phile )
							modified_backup_size = modified_backup_size - size_of_backup
						else:
							print '  %s Days till deletion of : %s %s Gigs' % ( 
								( delete_backups_x_days_old - phile_age.days ), 
								phile, 
								size_of_backup 
							)

				for deletion in marked_for_deletion:
					phile_path  = os.path.join( pulldown_location, deletion )				
					print '  Removing ', phile_path
					if os.path.isdir( phile_path ):				
						shutil.rmtree(  phile_path )
					else:
						os.remove( phile_path )

	def notify( self ):
		# print 'Script Now Time:   ', datetime.datetime.now()
		# print 'Script Start Time: ', script_start_time
		script_time = datetime.datetime.now() - script_start_time
		if not self.args.skip_notify and script_time > datetime.timedelta( minutes = 2 ):
			if use_push_bullet:
				title   = 'Data Finished Downloading'
				message = "Download was %s Gigs and %s rows in %s.\n" % ( 
					self.download_size['gigs'], 
					self.download_size['rows'], 
					str ( datetime.datetime.now() - script_start_time )  )
				message += "Downloaded\n"
				for database, info in self.downloads.iteritems():
					message += '  ' + database + '\n'
					for table in info['tables']:
						message += '    ' + table
				BoojPushBullet().send( use_push_bullet_key, title, message )

	def __set_db( self, method, db_key ):
		chosen_database = None
		if db_key == False:
			print 'the database key = none'
			print ' '
			sys.exit()
		if db_key == 'file':
			return False
		for db in databases.iterkeys():
			if db_key == db:
				chosen_database = databases[db_key]
				if method == 'dest' and db_key in no_write_databases:
					print 'ERROR cannot set %s as destination Database, dick!' % db_key
					sys.exit()
				break
		return chosen_database

	def __set_download_dir( self ):
		for database, info in self.downloads.iteritems():
			dir_suffix = database + '_' + info['tables'][0]
			break
		the_dl_package  = script_start_time.strftime("%Y_%m_%d_%H_%M_%S_") + dir_suffix
		self.dl_package = the_dl_package
		the_dir  = os.path.join( pulldown_location, the_dl_package )
		if os.path.isdir( the_dir ) == False:
			os.makedirs( the_dir )
		return the_dir

	def __get_size( self, start_path ):
	    total_size = 0
	    for dirpath, dirnames, filenames in os.walk(start_path):
	        for f in filenames:
	            fp = os.path.join(dirpath, f)
	            total_size += os.path.getsize(fp)
	    total_size = ( ( total_size / 1024 ) / 1024 ) / 1024
	    return total_size		

def parse_args( args ):
	parser = ArgumentParser(description='')
	parser.add_argument('-v', '--verbosity', action='store_true', default=False, help='Enable verbosity')	
	parser.add_argument('-d', '--database', default=False, help='Database Schema to pull down')
	parser.add_argument('-t', '--tables', default=False, help='Tables to pull down')
	parser.add_argument('-s', '--set', default=False, help='The named set to pull down')
	parser.add_argument('-s', '--sourceDB', default=default_source_db, help='Source Database')
	parser.add_argument('-dt', '--destDB', default=default_dest_db, help='Destination Database')
	parser.add_argument('-n', '--name', default=False, help='Name the export file')	 #Not being used
	parser.add_argument('--no_backup', action='store_true', default=False, help='Disable backup of DB your importing to')
	parser.add_argument('--structure', action='store_true', default=False, help='Only pulldown database table strucutres, not data')
	parser.add_argument('--skip_notify', action='store_true', default=False, help='Skip sending notifications')
	parser.add_argument('-sv', '--skip_verify', action='store_true', default=False, help='Skip human verification of download')
	parser.add_argument('--debug', action='store_true', default=False, help='Enable the Debugger!')	
	args   = parser.parse_args()
	return args

if __name__ == "__main__":
	args = parse_args( sys.argv )
	PullDown( args ).run()

# End of File: database-pulldown.v2.py

# -*- coding: utf-8 -*-
"""
Started September 16, 2016
author: Rob Parry  (Rob.Parry@ars.usda.gov)

Parse LCB LTAR data from CR1000. Reformat it for NAL 
""" 
		
import os, sys
from datetime import datetime

# Globals
DEBUG = False
NO_OUTPUT = False
# Time zone parameter to add to time stamp
# TODO: add command line argument for time_zone
# TODO: actually use time_zone variable in constructing time stamp
time_zone = '-05:00'

def main(argv):
	global DEBUG, NO_OUTPUT, time_zone, time_last_data_read
	import getopt
	
	# staton number for this data file. This is just an initialization
	# number is pulled from input file name. See below for '-f' argument
	LCB_station_number = 2

	# folder containg log files
	datafile_folder = r'C:\Users\rparry\Documents\PythonScripts\LTARdata'
	file_name =  'Sta2LTAR15min.dat'
	# argv.append('-d')
	# argv.append(r'\\10.18.26.36\Meyers')

	try:
		opts, args = getopt.getopt(argv, 'DXhs:d:f:', ['debug', 'no-output', 'help', 'station-id', 'directory', 'filename'])
		if DEBUG: print ("opts: %s args: %s" % (opts, args) )
	except getopt.GetoptError:
		print ("Unrecognized command line option")
		usage()
		return(1)
	for opt, arg in opts:
		# print ("opt: [%s] arg: [%s]" % (opt,arg))
		if DEBUG: print ("procesing option: %s" % opt)
		if opt in ('-h', '--help'):
			usage()
			return(0)
		elif opt in ('-D', '--debug'):
			DEBUG = True
			print ("DEBUG now True")
		elif opt in ('-X', '--no-output'):
			NO_OUTPUT = True
		elif opt in ('-s', '--station-id'):
			LCB_station_number = int(arg)
		elif opt in ('-d', '--directory'):
			datafile_folder = arg
		elif opt in ('-f', '--filename'):
			file_name = arg
			# pull station number from file name - fourth character in file name
			# LIMITATION - can ONLY deal with a single digit station number!!!
			LCB_station_number = int(file_name[3])
			
	# build complete path 
	datafile_name = os.path.join(datafile_folder, file_name)
	# create 3 digit station id number as string
	LCB_station_string = "%3.3d" % LCB_station_number
	
	print ("data file name (short): %s" % file_name)
	if DEBUG:
		# print variables set by command line options and exit
		print ("LCB station number: %d, %s" % (LCB_station_number, LCB_station_string)  )
		print ("data file folder: %s" % datafile_folder)
		print ("entire data file name (long) %s" % datafile_name )
		
		
	# build output CSV file name based on station number and current month and year
	now = datetime.now()
	# alternate_date = datetime(2017, 4, 1)
	# if DEBUG: now = datetime(2016, 10, 1)
	output_file_name = 'lcbMET%3.3dL_01_%4.4d%2.2d00_00.csv' % (LCB_station_number, now.year, now.month )
	csv_file_name = os.path.join(datafile_folder, output_file_name)
	
	# read time the last data was read
	time_last_data_read = read_time_last_data_point( datafile_name )
	print ("last data read at [%s]" % time_last_data_read )
	
	# read data file
	print ("attempting to read data file [%s]" % (datafile_name) )
	all_data = read_datafile(datafile_name)
	# if DEBUG:
	# 	print ("last line of data:", all_data['timestamp'][-1])
	
	new_time_last_data_read = all_data['data'][-1][0]
	print ("new time last data read: [%s]" % new_time_last_data_read) 
	if (time_last_data_read == new_time_last_data_read):
		print (" >> times match - NO new data!! Not creating any new output files.")
		NO_OUTPUT = True

	if not NO_OUTPUT:
		# write out CSV file
		write_csv(all_data, csv_file_name, LCB_station_string)
		# write text file with instructions for FTP transfer
		write_ftp_instructions(csv_file_name)
	
		# write time last data point was read
		write_return_code = write_time_last_data_point( datafile_name, new_time_last_data_read )
		if write_return_code:
			print ("write of last data timestamp was successfull")
		else:
			print ("write of last data timestamp failed")

		# calculate and write MD5 hash		
		write_return_code = write_md5_hash(csv_file_name)
		if write_return_code:
			print ("write of MD5 hash file was successfull")
		else:
			print ("write of MD5 hash file failed")
	
	# done
	print ("done\n")
	return(0)
	
def usage():
	""" print usage message """
	print ("""parse_CR1000.py [-D | --debug] [-h | --help] [-s ID | --station-id ID] [-f | --folder]
	option:
	 -D     or  --debug       turn debug mode on
	 -h     or  --help        show this usage / help
	 -s id  or  --station-id  set station ID
	 -f folder_name or --folder folder_name  set folder name where input file is located
	 -X     or  --no-output   do not write any output
	""")
	
def read_datafile(filein):
	""" read data file 
	    Argument - input file name
		Returns -  diction with header, timestamp and data lists
	"""
	global time_last_data_read
	
	try:
		fp = open (filein)
	except:
		print ("failed to open file!!")
		sys.exit(1)
		
	in_header = True  # start with the header lines
	header_count = 0  # counter for header lines
	num_header_lines = 4  # number of header lines
	header = []       # list to hold header lines
	data_count = 0    # separate counter for data lines
	data = []         # list to hold data lines
	# get current month and year, only use data for current month

	now = datetime.now()
	# alternate_date = datetime(2017, 4, 1)
	# if DEBUG: now = datetime(2016, 10, 1)
	current_year_month = "%4.4d-%2.2d" % (now.year, now.month)
	
	month_roll_over = False    # did the month roll over from previous data read
	begin_read_data = False    # force when to begin reading data when month rolls over
	if current_year_month not in time_last_data_read:
		# this will be true when time of last data read was just before month roll over
		month_roll_over = True
		if DEBUG: print ("month roll over now True!")
	
	for line in fp:
		if header_count > ( num_header_lines - 1 ):
			in_header = False
			
		if in_header:
			header.append( line.split(',') )
			header_count += 1
		else:
			# ALL the data are stings at this point!!!
			line = line.rstrip()	          # removing trailing CR
			line = line.replace('"', '')    # remove double quotes
			line = line.replace('NAN', '')  # replace NAN with blank
			# checked order of precedence - do not need parenthesis
			# only data from current year-month or if just did a month roll over
			if current_year_month in line.split(',')[0] or begin_read_data:
				data.append( line.split(',') )  # split on commas and add to list
				data_count += 1
				# need to unset begin_read_data after first use so that it
				# does not impact the first condition of if statement -
				# which will happen if rolling over to yet another month (reprocessing old data)
				begin_read_data = False
			if month_roll_over and time_last_data_read in line.split(',')[0]:
				begin_read_data = True
			
	fp.close()
	print ("read in %d header lines and %d data lines" % (header_count, data_count) )

	# reformat time stamp - add 'T' and time zone offset
	timestamp = [ data[i][0].replace(' ', 'T') + '-05:00' for i in range(len(data)) ]
# 	timestamp = [ data[i][0] + '-05:00' for i in range(len(timestamp)) ]
	# column to print
	# c = 1
	# print ("head and data from column %d" % (c) )
	# header_output = [ header[i][c]  for i in range(4) ]
	# data_output = [ data[i][c]  for i in range(1) ]
	# print (header_output )
	# print (data_output)
	# print ("number of columns for header and data")
	# num_header_columns = [ len(header[i]) for i in range(len(header)) ]
	# print ( num_header_columns )
	# num_data_columns = [ len(data[i]) for i in range(len(data)) ]
	# print ( num_data_columns )
	all_data = {}
	all_data['header'] = header
	all_data['timestamp'] = timestamp
	all_data['data'] = data
	# print (data)
	# for t in timestamp:
	# 	print ('%s' % t )
	
	return(all_data)
			
def write_csv(all_data, csv_file_name, LCB_station_string):
	# LTARSiteAcronym == LCB
	# StationID == 002 # changed April 25, 2017 to use LCB_station_string
	# RecordType == L   as per Jeff Campbell email Sept 26, 2016
	# data format document: "C:\Users\rparry\Documents\Met data record definiton 2.docx"
	# need all the columns, leave blank or space still separated by commas

	print ("attempting to write to file [%s]" % (csv_file_name) )	
	try:
		csv_fp = open(csv_file_name, 'w')
	except:
		print ("failed to open CSV file")
		sys.exit(1)

	# force 4 places for floating point for certain fields
	fields = [2, 3,5, 6, 12, 19, 20, 21, 23, 25, 27]
	float_4places = []
	# need to initialize the float4_places list
	for i in range(max(fields) + 1):
		float_4places.append(i)
		float_4places[i] = []
		
	# then convert those fields to floats
	for c in fields:
		float_4places[c] = [ "%.4f" % float(all_data['data'][i][c])   for i in range(len(all_data['data'])) ]

	# header for CSV file
	header = 'LTARSiteAcronym,StationID,DateTime,RecordType,AirTemperature,WindSpeed,WindDirection,RelativeHumidity,Precipitation,AirPressure,PAR,ShortWaveIn,LongWaveIn,BatteryVoltage,LoggerTemperature,ObservationInterval,AirTemperatureQC,WindSpeedQC,WindDirectionQC,RelativeHumidityQC,PrecipitationQC,AirPressureQC,PARQC,ShortWaveInQC,LongWaveInQC,BatteryVoltageQC,LoggerTemperatureQC,AirTempMinQC,AirTempMaxQC,QCPerson,LastUpdate,QAQCLevel,RequestDate,AirTempMin,AirTempMinTime,AirTempMax,AirTempMaxTime,Action\n'
	csv_fp.write(header)
	# initialize variables for writing data to file
	num_lines = 0
	prev_line_timestamp = 'initialze variable'
	records_per_day = {}
	for i in range(len(all_data['data'])):
		if prev_line_timestamp == all_data['timestamp'][i]:
			print ( "   duplicate data found at timestamp [%s]" % (all_data['timestamp'][i]) )
		else:
			# get timestamp as string to use as key for records_per_day dictionary
			ts_string = all_data['timestamp'][i][:10]
			if ts_string not in records_per_day: records_per_day[ts_string] = 0
			records_per_day[ts_string] += 1
			# csv_fp.write(','.join( ('LCB', '002',  all_data['timestamp'][i], 'L', all_data['data'][i][2], all_data['data'][i][5], all_data['data'][i][6], all_data['data'][i][3], all_data['data'][i][12], all_data['data'][i][19], all_data['data'][i][27], '', '', all_data['data'][i][21], all_data['data'][i][20], '15', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', all_data['data'][i][25], reformat_timestamp(all_data['data'][i][26]), all_data['data'][i][23], reformat_timestamp(all_data['data'][i][24]) ) ) )
			csv_fp.write(','.join( ('LCB', LCB_station_string,  all_data['timestamp'][i], 'L', float_4places[2][i], float_4places[5][i], float_4places[6][i], float_4places[3][i], float_4places[12][i], float_4places[19][i], float_4places[27][i], '', '', float_4places[21][i], float_4places[20][i], '15', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', float_4places[25][i], reformat_timestamp(all_data['data'][i][26]), float_4places[23][i], reformat_timestamp(all_data['data'][i][24]), 'A' ) ) )
			csv_fp.write('\n')
			
		prev_line_timestamp = all_data['timestamp'][i]
		num_lines += 1
		
	csv_fp.close()
	print ("wrote %d lines of data" % num_lines)
	for ts in records_per_day:
		if records_per_day[ts] != 96:
			print ("  less than 96 records in day %s : %d" % (ts, records_per_day[ts]) )

def reformat_timestamp(datetime_str):
	""" reformat time stamp from YYYY-MM-DD HH:MM:SS to YYY-MM-DDTHH:MM:SS-05:00
		currently NO option to change the timezone
	    arguments - one: incoming time stamp
		returns - reformatted time stamp
	"""
	fixed_timestamp = datetime_str.replace(' ', 'T') + '-05:00'
	return(fixed_timestamp)

def read_time_last_data_point(datafile_name):
	""" read the time the last data was collected.
		The file name used has replaced the extension .dat with .last 
	    agruments - one: file name of data file
		returns - time last data was collected. Time is formatted as it
		          appears in data file
	"""
	last_data_file_name = datafile_name.replace('.dat', '.last')
	# print ("attempting to read file [%s]" % last_data_file_name)
	# try to open file, if it doesn't exist, return something meaningful
	time_last_data_read = ""
	try:
		fp = open(last_data_file_name, 'r')
	except:
		return( "no time found" )
	
	time_last_data_read = fp.read()
	fp.close()
	return(time_last_data_read)
	
def write_time_last_data_point(datafile_name, time_string):
	""" write the time the last data point was collected
		arguments - two: file name of data file and time string to write to file
		returns - True or False if write was successful or not
	"""
	last_data_file_name = datafile_name.replace('.dat', '.last')
	try:
		fp = open(last_data_file_name, 'w')
	except:
		print("ERROR in function write_time_last_data_point:")
		print("      not able to open file for writting [%s]" % last_data_file_name)
		return(False)
	fp.write(time_string)
	fp.close()
	
	return(True)
	
def write_md5_hash(csv_file_name):
	""" compute and write the MD5 hash for the csv file """
	import hashlib
	
	md5_file_name = csv_file_name.replace('.csv', '.md5')
	md5_hash = hashlib.md5(open(csv_file_name, 'rb').read()).hexdigest()
	
	try:
		fp = open(md5_file_name, 'w')
	except:
		print("ERROR in function write_md5_hash:")
		print("      not able to open file for writting [%s]" % md5_file_name)
		return(False)
	fp.write(md5_hash)
	fp.close()
	
	return(True)
	
def write_ftp_instructions(csv_file_name):
	""" write file with FTP instructions. 
		Read in template file and use current file names
	    Files to transfer vary based on month and year """

	md5_file_name = csv_file_name.replace('.csv', '.md5')
	ftp_template = 'transfer_to_FTP_server_template.txt'
	ftp_instructions = 'transfer_to_FTP_server.txt'
	print ('attempting to read FTP template file [%s]' % ftp_template)

	try:
		ftp_fp = open(ftp_template, 'r')
	except:
		print("ERROR in function write_ftp_instructions:")
		print("      not able to open file for reading [%s]" % ftp_template)
		return(False)
		
	# print ('lines from FTP template file:')
	output = []
	for line in ftp_fp.readlines():
		line_clean = line.rstrip()
		# print (line_clean)
		if 'put' in line_clean:
			# change file names for the two put lines
			if 'csv' in line_clean:
				line_clean = 'put "%s"' % (csv_file_name)
			if 'md5' in line_clean:
				line_clean = 'put "%s"' % (md5_file_name)
			# print ('line_clean: [%s]' % line_clean)
		output.append(line_clean)
			
	ftp_fp.close()
	
	print ('attempting to write FTP instruction file [%s]' % ftp_instructions)

	try:
		ftp_fp = open(ftp_instructions, 'a')
	except:
		print("ERROR in function write_ftp_instructions:")
		print("      not able to open file for writting [%s]" % ftp_instructions)
		return(False)
		
	for line in output:
		# print line
		ftp_fp.write(line + '\n')
	ftp_fp.close()
	
	return(True)
	
# ====================================	
if __name__ == '__main__':
	main( sys.argv[1:] )

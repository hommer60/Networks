import argparse
import socket
import sys
import os
from math import floor
import threading

def create_ftp_messages(args):
	messages = []
	messages.append("RETR " + args['file'] + "\n")

	## SET READ POINT FOR PARALLEL DATA SOCKET
	if 'byte_offset' in args and args['byte_offset'] != None:
		messages.append("REST " + args['byte_offset'] + "\n")

	## GET SIZE FOR PARALLEL CONTROL SOCKET
	if 'config' in args and args['config'] != None:
		messages.append("SIZE " + args['file'] + '\n')

	messages.append("PASV\n")
	messages.append("TYPE " + args['mode'] + "\n")
	messages.append("PASS " + args['password'] + "\n")
	messages.append("USER " + args['username'] + "\n")
	return messages

def process_config_file(config_file, num_bytes):
	with open(config_file) as f:
		lines = f.readlines()
		lines = [x.strip() for x in lines]
		thread_data = []
		bytes_per_file = floor(num_bytes/len(lines))
		for i in range(len(lines)):
			line = lines[i]
			args = {}
			args['file'] = line.split('/')[3]
			args['mode'] = 'I'
			args['password'] = line.split('/')[2].split(':')[1].split('@')[0]
			args['username'] = line.split('/')[2].split(':')[0]
			args['byte_offset'] = str(i*bytes_per_file)
			server = line.split('/')[2].split(':')[1].split('@')[1]
			port = 21
			messages = create_ftp_messages(args)
			write_file = ".tmp_" + str(i)
			thread_data.append([messages, server, port, write_file, bytes_per_file])
		## LAST THREAD SHOULD JUST READ REMAINING BYTES	
		thread_data[-1][4] = None
		f.close()
		return thread_data


def setup_socket(host, port):
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	except socket.error as err:
		sys.stderr.write("Can't connect to server")
		exit(1)

	try:
		ip = socket.gethostbyname(host)
	except socket.gaierror:
		sys.stderr.write("Can't connect to server")
		exit(1)

	s.connect((ip, port))
	return s

def handle_error_code(code):
	if code == "530":
		sys.stderr.write("Authentication failed")
		exit(2)
	elif code == "500":
		sys.stderr.write("Command not implemented by server")
		exit(5)
	elif code == "550":
		sys.stderr.write("File not found")
		exit(3)
	elif code == "425":
		sys.stderr.write("Operation not allowed by server")
		exit(6)
	else:
		return

## Extract host + port from ftp server message
def parse_connect_info(server_msg):

	connect_info = server_msg.split("(")[1].split(")")[0].split(",")
	host = '.'.join(connect_info[0:4])
	port = (int(connect_info[4]) * 256) + int(connect_info[5])
	return host, port

def write_file(data_socket, read_bytes, out_file, is_binary):
	if read_bytes == None:
		read_bytes = float('inf')
	read_buffer = 1024

	mode = 'wt'
	if is_binary == True:
		mode = 'wb'

	with open(out_file, mode) as f:
	    while True and f.tell() < read_bytes:
	    	if read_bytes - f.tell() < 1024:
	    		read_buffer = read_bytes - f.tell()
	    	d = data_socket.recv(read_buffer)
	    	if not d:
	    		break
	    	f.write(d)
	    f.close()

def run(messages, server, port, out_file, config_file, read_bytes, is_binary):
	control_socket = setup_socket(server, port)
	while len(messages) > 0:
		data = control_socket.recv(1024)
		if not data:
			break
		data = data.decode()
		code = data[:3]
		handle_error_code(code)

		if code == "227" and config_file == None:
			host, port = parse_connect_info(data)
			data_socket = setup_socket(host, port)
			if 'REST' in messages[-1]:
				## In Parallel Mode, SET FILE READ INDEX
				control_socket.send(messages.pop().encode())
				data = control_socket.recv(1024)
				data = data.decode()
				code = data[:3]
				handle_error_code(code)

			## Send RETR command
			control_socket.send(messages.pop().encode())
			data = control_socket.recv(1024)
			data = data.decode()
			code = data[:3]
			handle_error_code(code)

			write_file(data_socket, read_bytes, out_file, is_binary)
			data_socket.close()
			control_socket.close()

			sys.stderr.write("Operation successfully completed")
			return
		elif code == "213" and config_file != None:
			## FOUND SIZE OF FILE
			file_size = int(data.split(" ")[1])
			control_socket.close()
			thread_data = process_config_file(config_file, file_size)
			run_parallel_threads(thread_data, out_file)
			return

		message = messages.pop().encode()
		control_socket.send(message)

	control_socket.close()


def run_parallel_threads(thread_data, out_file):

	threads = []

	## Create and start each thread
	for item in thread_data:
		[messages, server, port, tmp_out_file, read_bytes] = item
		config_file = None
		is_binary = True
		thread = threading.Thread(target=run, args=(messages, server, port, tmp_out_file, config_file, read_bytes, is_binary))
		thread.start()
		threads.append(thread)

	## Wait for threads to terminate
	for thread in threads:
		thread.join()

	## Combine each thread file
	with open(out_file, 'wb') as f:
		for i in range(0, len(threads)):
			with open('.tmp_' + str(i), 'rb') as infile:
				for line in infile:
					f.write(line)
				infile.close()

				## Remove tmp thread file
				os.remove('.tmp_' + str(i))

	f.close()

def main(args):
	server = args['server']
	port = args['port']
	out_file = args['file']
	config_file = args['config']
	mode = args['mode']
	is_binary = True
	if mode == 'ASCII':
		args['mode'] = 'A'
		is_binary = False
	elif mode == 'binary' or mode == 'I':
		args['mode'] = 'I'
	else:
		sys.stderr.write("SYNTAX ERROR")
		exit(99)
	messages = create_ftp_messages(args)
	run(messages, server, port, out_file, config_file, None, is_binary)

parser = argparse.ArgumentParser()
req_arguments = parser.add_argument_group('Required Arguments')
req_arguments.add_argument("--server", "-s", help="Specifies the server to download the file from",
                    type=str, required=True)
req_arguments.add_argument("--file", "-f", help="Specifies the file to download.",
                    type=str, required=True)
parser.add_argument('--version', action='store_true', help="display the version of this application")
parser.add_argument('--port', help="Specifies the port to be used when contacting the server. (default value: 21)", type=int, default=21)
parser.add_argument('--config', "-c", help="config", type=str, default=None)
parser.add_argument('--username', '-n',  help="Specifies the username when logging into the FTP server (default value: anonymous).", type=str, default='anonymous')
parser.add_argument('-P', '--password',  help="Specifies the password when logging into the FTP server (default value: user@localhost.localnet).", type=str, default='user@localhost.localnet')
parser.add_argument('--mode',  help="Specifies the mode to be used for the transfer (ASCII or binary) (default value: binary)", type=str, default='I')
parser.add_argument('--log',  help='Logs all the FTP commands exchanged with the server and the corresponding replies to file logfile.')
args = parser.parse_args()
main(vars(args))
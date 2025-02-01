#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
 python kocom script

 : forked from script written by 따분, Susu Daddy

 apt-get install mosquitto
 python3 -m pip install pyserial
 python3 -m pip install paho-mqtt
'''
import os
import time
import platform
import threading
import queue
import random
import json
import paho.mqtt.client as mqtt
import logging
import configparser


# define -------------------------------

CONFIG_FILE = 'kocom.conf'
BUF_SIZE = 100

read_write_gap = 0.03  # minimal time interval between last read to write
polling_interval = 300   #300  # polling interval

header_h = 'f7'

type_t_dic = {'30b':'send', '30d':'ack'}
seq_t_dic = {'c':1, 'd':2, 'e':3, 'f':4}
type_h_dic = {v: k for k, v in type_t_dic.items()}
seq_h_dic = {v: k for k, v in seq_t_dic.items()}

device_t_dic = {'0e':'light', '12':'gas', '32':'fan', '33':'lightall', '36':'thermo', '3a':'realtime'} 
room_t_dic = {'01':'one', '11':'livingroom', '12':'room1', '13':'room2', '14':'room3', '1f':'all'}
light_t_dic = {'11':'1', '12':'2', '13':'3', '14':'4', '1f':'all', 'ff':'all'}
cmd_t_dic = {'41':'set_onoff', '42':'set_speed', '43':'set_thermo', '44':'set_temp', '45':'set_away',
			'c1':'state_onoff', 'c2':'state_speed', 'c3':'state_thermo', 'c4':'state_temp', 'c5':'state_away',
			'01':'query', '81':'state'}

device_h_dic = {v: k for k, v in device_t_dic.items()}
room_h_dic = {v: k for k, v in room_t_dic.items()}
light_h_dic = {v: k for k, v in light_t_dic.items()}
cmd_h_dic = {v: k for k, v in cmd_t_dic.items()}

# mqtt functions ----------------------------

def init_mqttc():
    mqttc = mqtt.Client()
    mqttc.on_message = mqtt_on_message
    mqttc.on_subscribe = mqtt_on_subscribe
    mqttc.on_connect = mqtt_on_connect
    mqttc.on_disconnect = mqtt_on_disconnect

    if config.get('MQTT','mqtt_allow_anonymous') != 'True':
        logging.info("[MQTT] connecting using username and password")
        mqttc.username_pw_set(username=config.get('MQTT','mqtt_username',fallback=''), password=config.get('MQTT','mqtt_password',fallback=''))
    mqtt_server = config.get('MQTT','mqtt_server')
    mqtt_port = int(config.get('MQTT','mqtt_port'))
    mqttc.connect(mqtt_server, mqtt_port, 60)
    mqttc.loop_start()
    return mqttc

def mqtt_on_subscribe(mqttc, obj, mid, granted_qos):
    logging.info("[MQTT] Subscribed: " + str(mid) + " " + str(granted_qos))

def mqtt_on_log(mqttc, obj, level, string):
    logging.info("[MQTT] on_log : "+string)

def mqtt_on_connect(mqttc, userdata, flags, rc):
    if rc == 0:
        logging.info("[MQTT] Connected - 0: OK")
        mqttc.subscribe('kocom/#', 0)
    else:
        logging.error("[MQTT] Connection error - {}: {}".format(rc, mqtt.connack_string(rc)))

def mqtt_on_disconnect(mqttc, userdata,rc=0):
    logging.error("[MQTT] Disconnected - "+str(rc))


# serial/socket communication class & functions--------------------

class RS485Wrapper:
    def __init__(self, serial_port=None, socket_server=None, socket_port=0):
        if socket_server == None:
            self.type = 'serial'
            self.serial_port = serial_port
        else:
            self.type = 'socket'
            self.socket_server = socket_server
            self.socket_port = socket_port
        self.last_read_time = 0
        self.conn = False

    def connect(self):
        self.close()
        self.last_read_time = 0
        if self.type=='serial':
            self.conn = self.connect_serial(self.serial_port)
        elif self.type=='socket':
            self.conn = self.connect_socket(self.socket_server, self.socket_port)
        return self.conn

    def connect_serial(self, SERIAL_PORT):
        if SERIAL_PORT==None:
            os_platfrom = platform.system()
            if os_platfrom == 'Linux':
                SERIAL_PORT = '/dev/ttyUSB0'
            else:
                SERIAL_PORT = 'com3'
        try:
            ser = serial.Serial(SERIAL_PORT, 9600, timeout=1)
            ser.bytesize = 8
            ser.stopbits = 1
            if ser.is_open == False:
                raise Exception('Not ready')
            logging.info('Serial connected : {}'.format(ser))
            return ser
        except Exception as e:
            logging.error('Serial open failure : {}'.format(e))
            return False

    def connect_socket(self, SOCKET_SERVER, SOCKET_PORT):
        sock = socket.socket()
        sock.settimeout(10)
        try:
            sock.connect((SOCKET_SERVER, SOCKET_PORT))
        except Exception as e:
            logging.error('Socket connection failure : {} | server {}, port {}'.format(e, SOCKET_SERVER, SOCKET_PORT))
            return False
        logging.info('socket connected | server {}, port {}'.format(SOCKET_SERVER, SOCKET_PORT))
        sock.settimeout(polling_interval+15)   # set read timeout a little bit more than polling interval
        return sock

    def read(self):
        if self.conn == False:
            return ''
        ret = ''
        if self.type=='serial':
            for i in range(polling_interval+15):
                try:
                    ret = self.conn.read()
                except AttributeError:
                    raise Exception('exception occured while reading serial')
                except TypeError:
                    raise Exception('exception occured while reading serial')
                if len(ret) != 0:
                    break
        elif self.type=='socket':
            ret = self.conn.recv(1)

        if len(ret) == 0:
            raise Exception('read byte errror')
        else:
            self.last_read_time = time.time()
        return ret

    def write(self, data):
        if self.conn == False:
            return False
        if self.last_read_time == 0:
            time.sleep(1)
        while time.time() - self.last_read_time < read_write_gap:
            #logging.debug('pending write : time too short after last read')
            time.sleep(max([0, read_write_gap - time.time() + self.last_read_time]))
        if self.type=='serial':
            return self.conn.write(data)
        elif self.type=='socket':
            return self.conn.send(data)
        else:
            return False

    def close(self):
        ret = False
        if self.conn != False:
            try:
                ret = self.conn.close()
                self.conn = False
            except:
                pass
        return ret

    def reconnect(self):
        self.close()
        while True: 
            logging.info('reconnecting to RS485...')
            if self.connect() != False:
                break
            time.sleep(10)


def send(dest, cmd, value, log=None, check_ack=True):
    send_lock.acquire()
    ack_data.clear()
    ret = False
    for i in range(2):
        send_data = header_h + dest + cmd + '{0:02x}'.format(int(len(value)/2)) + value
        send_data += chkxor(send_data)
        send_data += chksum(send_data)
        try:
            if rs485.write(bytearray.fromhex(send_data)) == False:
                raise Exception('Not ready')
        except Exception as ex:
            logging.error("*** Write error.[{}]".format(ex) )
            break
        if log != None:
            logging.info('[SEND|{}] {}'.format(log, send_data))
        if check_ack == False:
            time.sleep(1)
            ret = send_data
            break

        # wait and checking for ACK
        ack_data.append(dest)
        try:
            recv = ack_q.get(True, 0.3+0.2*random.random()) # random wait between 0.3~0.5 seconds for ACK
            if config.get('Log', 'show_recv_hex') == 'True':
                logging.info ('[ACK] OK')
            ret = send_data
            break
        except queue.Empty:
            pass

    if ret == False:
        logging.info('send failed. closing RS485. it will try to reconnect to RS485 shortly.')
        rs485.close()
    ack_data.clear()
    send_lock.release()
    return ret


def chksum(data_h):
    sum_buf = sum(bytearray.fromhex(data_h))
    return '{0:02x}'.format((sum_buf)%256)  # return chksum hex value in text format

def chkxor(data_h):
    sum = 0
    for e in bytearray.fromhex(data_h):
        sum ^= e
    return '{0:02x}'.format((sum)%256)  # return chksum hex value in text format


# hex parsing --------------------------------

def parse(hex_data):
	header_h = hex_data[:2]
	dest_h = hex_data[2:6]
	cmd_h = hex_data[6:8]
	count_h = hex_data[8:10]
	count = int(count_h, 16)
	
	if (count == 0):
		data_end = 10
		data_h = None
	else:
		data_end = 10 + 2*count
		data_h = hex_data[10:data_end]

	chkxor_h = hex_data[data_end:data_end + 2]  # checkxor
	chksum_h = hex_data[data_end + 2:data_end + 4]  # checksum
		
	cmd = cmd_t_dic.get(cmd_h)

	ret = {'header_h':header_h, 'dest_h':dest_h, 'cmd_h':cmd_h, 'count':count,
		'data_h':data_h,'chkxor_h':chkxor_h, 'chksum_h':chksum_h, 
		'dest':device_t_dic.get(dest_h[:2]),
		'room_id':room_t_dic.get(dest_h[2:4]),
		'light_id':light_t_dic.get(dest_h[2:4]),
		'cmd':cmd if cmd!=None else cmd_h,
		'value':data_h,
		'time': time.time(),
		'flag':None}
	return ret


def thermo_parse(value):
	ret = {'heat_mode': 'off' if value[2:4]=='00' else 'heat', 'away': 'true' if value[4:6]=='ff' else 'false'}
	if len(value) == (0x0d * 2):   # when all rooms
		for i in range(1,5):
			ret['set_temp_'+str(i)] = int(value[6+4*i:8+4*i], 16)
			ret['cur_temp_'+str(i)] = int(value[8+4*i:10+4*i], 16)
	return ret


def light_parse(value):
	ret = {}
	count = int(len(value)/2)   # when all lights
	for i in range(1, count):
		ret['light_'+str(i)] = 'off' if value[i*2:i*2+2] == '00' else 'on'
	return ret


def fan_parse(value):
    level_dic = {'01':'1', '02':'2', '03':'3'}
    state = 'off' if value[2:4] == '00' else 'on'
    level = '0' if state == 'off' else level_dic.get(value[4:6])
    return { 'state': state, 'level': level}

def utility_parse(value):
	ret = {}
	ret['electric'] = int(value[4:10], 16)   # ??
	return ret

# query device --------------------------

def query(device_h, publish=False):
    # find from the cache first
    for c in cache_data:
        if time.time() - c['time'] > polling_interval:  # if there's no data within polling interval, then exit cache search
            break
        if c['dest_h']==device_h and c['cmd']!='query':
            if (config.get('Log', 'show_query_hex')=='True'):
                logging.info('[cache|{}{}] query cache {}'.format(c['dest'], c['room_id'], c['data_h']))
            return c  # return the value in the cache

    # if there's no cache data within polling inteval, then send query packet
    if (config.get('Log', 'show_query_hex')=='True'):
        log = 'query ' + device_t_dic.get(device_h[:2]) + ' ' + room_t_dic.get(device_h[2:4])
    else:
        log = None
    return send_wait_response(dest=device_h, cmd=cmd_h_dic['query'], log=log, publish=publish)


#def send_wait_response(dest, cmd=cmd_h_dic['query'], value='', log=None, check_ack=True, publish=True):
def send_wait_response(dest, cmd=cmd_h_dic['query'], value='', log=None, check_ack=False, publish=True):
    #logging.debug('waiting for send_wait_response :'+dest)
    wait_target.put(dest)
    #logging.debug('entered send_wait_response :'+dest)
    ret =  { 'value':'00', 'flag':False }

    if send(dest, cmd, value, log, check_ack) != False:
        try:
            ret = wait_q.get(True, 1)   # timeout: 10
            if publish==True:
                publish_status(ret)
        except queue.Empty:
            pass
    wait_target.get()
    #logging.debug('exiting send_wait_response :'+dest)
    return ret


#===== parse MQTT --> send hex packet ===== 

def mqtt_on_message(mqttc, obj, msg):
    command = msg.payload.decode('ascii')
    topic_d = msg.topic.split('/')

    # do not process other than command topic
    if topic_d[-1] != 'command':
        return         

    logging.info("[MQTT RECV] " + msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

    # thermo heat/off : kocom/room/thermo/heat_mode/command
    if 'thermo' in topic_d and 'heat_mode' in topic_d:
        heatmode_dic = {'heat':'01', 'on':'01', 'off':'00'} 
        dev_id = device_h_dic['thermo'] + room_h_dic[topic_d[1]]
        cmd = cmd_h_dic['set_thermo']
        value = heatmode_dic.get(command) 
        log = 'thermo heatmode'

    # thermo set temp : kocom/room/thermo/set_temp/command
    elif 'thermo' in topic_d and 'set_temp' in topic_d:
        dev_id = device_h_dic['thermo'] + room_h_dic[topic_d[1]]
        cmd = cmd_h_dic['set_temp']
        value = '{0:02x}'.format(int(float(command)))
        log = 'thermo settemp'

    # light on/off : kocom/livingroom/light/3/command
    elif 'light' in topic_d:
        dev_id = device_h_dic['light'] + light_h_dic[topic_d[3]]
        if topic_d[3] == 'all':
            cmd = cmd_h_dic['set_speed']   # on/off all
        else:
            cmd = cmd_h_dic['set_onoff']
        value = '01' if command == 'on' else '00'
        log = 'light'

    # gas off : kocom/livingroom/gas/command
    elif 'gas' in topic_d:
        dev_id = device_h_dic['gas'] + room_h_dic.get('one')
        cmd = cmd_h_dic['set_onoff']
        value = '00'
        log = 'gas'

        if command != 'off':
            logging.info('You can only turn off gas.')

    # kocom/livingroom/fan/command
    elif 'fan' in topic_d:
        dev_id = device_h_dic['fan'] + room_h_dic.get('one')
        onoff_dic = {'off':'00', 'on':'01'}
        speed_dic = {'1':'01', '2':'02', '3':'03'}
        if command == '0':
            command = 'off'

        if command in onoff_dic.keys(): # fan on off with previous speed 
            cmd = cmd_h_dic['set_onoff']
            value = onoff_dic.get(command)
        elif command in speed_dic.keys(): # fan on with specified speed
            cmd = cmd_h_dic['set_speed']
            value = speed_dic.get(command)
        log = 'fan'

    send_wait_response(dest=dev_id, cmd=cmd, value=value, log=log)
    #print(f'{log}: f7{dev_id}{cmd}01{value}xxxx')

#===== parse hex packet --> publish MQTT ===== 

def publish_status(p):
    threading.Thread(target=packet_processor, args=(p,)).start()

def packet_processor(p):
    state = {}
    if p['cmd'].find('state') == 0:   # match a string that starts with 'state'
        if p['dest'] == 'thermo':
            state = thermo_parse(p['value'])
            mqttc.publish("kocom/" + p['room_id'] + "/thermo/state", json.dumps(state))
        elif p['dest'] == 'light':
            state = light_parse(p['value'])
            mqttc.publish("kocom/livingroom/light/" + p['light_id'] + "/state", json.dumps(state))
        elif p['dest'] == 'fan':
            state = fan_parse(p['value'])
            mqttc.publish("kocom/livingroom/fan/state", json.dumps(state))    
        elif p['dest'] == 'gas':
            state = {'state': p['cmd']}
            mqttc.publish("kocom/livingroom/gas/state", json.dumps(state))
        #logging.info('[MQTT publish|{} {} data[{}]'.format(p['dest'], p['room_id'], state))
        #logging.info('[MQTT publish|{} {}]'.format(p['dest'], p['room_id']))

#===== thread functions ===== 

def poll_state():
    global poll_timer
    poll_timer.cancel()

    dev_list = [x.strip() for x in config.get('Device','enabled').split(',')]
    no_polling_list = ['wallpad', 'elevator']

    #thread health check
    for thread_instance in thread_list:
        if thread_instance.is_alive() == False:
            logging.error('[THREAD] {} is not active. starting.'.format( thread_instance.name))
            thread_instance.start()

    for t in dev_list:
        dev = t.split('_')
        if dev[0] in no_polling_list:
            continue

        dev_id = device_h_dic.get(dev[0])
        if len(dev) > 1:
            sub_id = room_h_dic.get(dev[1])
        else:
            sub_id = room_h_dic.get('all')
        
        if dev_id != None and sub_id != None:
            if query(dev_id + sub_id, publish=True)['flag'] == False:
                break
            time.sleep(1)

    poll_timer.cancel()
    poll_timer = threading.Timer(polling_interval, poll_state)
    poll_timer.start()


def read_serial():
	global poll_timer
	buf = ''
	not_parsed_buf = ''
	packet_size = 14   # min size
	while True:
		try:
			d = rs485.read()
			hex_d = '{0:02x}'.format(ord(d))

			buf += hex_d
			#if buf[:len(header_h)] != header_h[:len(buf)]:
			if buf[:2] != header_h:
				not_parsed_buf += buf
				buf=''
				continue
			else:
				if not_parsed_buf != '':
					logging.info('[comm] not parsed '+not_parsed_buf)
					not_parsed_buf = ''

			if len(buf) >= 10:
				data_size = int(buf[8:10], 16)
				packet_size = data_size*2 + 14
			else:
				continue

			if len(buf) >= packet_size:
				chksum_calc = chksum(buf[:packet_size - 2])
				chksum_buf = buf[packet_size - 2:packet_size]
				if chksum_calc == chksum_buf:
					if msg_q.full():
						logging.error('msg_q is full. probably error occured.')
					msg_q.put(buf)  # valid packet
					if len(buf) > packet_size:
						buf = buf[packet_size:]
					else:
						buf=''
				else:
					logging.info("[comm] invalid packet: expected checksum {}".format(chksum_calc))
					frame_start = buf.find(header_h, len(header_h))
					if frame_start < 0:
						not_parsed_buf += buf
						buf=''
					else:
						not_parsed_buf += buf[:frame_start]
						buf = buf[frame_start:]
		except Exception as ex:
			logging.error("*** Read error.[{}]".format(ex) )
			poll_timer.cancel()
			del cache_data[:]
			rs485.reconnect()
			poll_timer = threading.Timer(1, poll_state)
			poll_timer.start()


def listen_hexdata():
	while True:
		d = msg_q.get()

		#if config.get('Log', 'show_recv_hex') == 'True':
		#	logging.info("[recv] " + d)

		p_ret = parse(d)

		# don't log the messages originated from wallpad (state check)
		#if p_ret['dest_h'][:2] == '0e' and p_ret['cmd'].find('state') == 0:   # light
		#if p_ret['dest_h'][:2] == '32' and p_ret['cmd'].find('state') == 0:   # fan
		#if p_ret['dest_h'][:2] == '33' and p_ret['cmd'].find('state') == 0:   # lightall
		#if p_ret['dest_h'][:2] == '36' and p_ret['cmd'].find('state') == 0:   # thermo
		#	print(d)

		# store recent packets in cache
		cache_data.insert(0, p_ret)
		if len(cache_data) > BUF_SIZE:
			del cache_data[-1]

		if 0:   # Why doesn't this work ???
			if p_ret["data_h"] in ack_data:
				ack_q.put(d)
				continue
		else:
			is_in = False
			for e in ack_data:
				if(e == p_ret['dest_h']):
					is_in = True
			if is_in:
				ack_q.put(d)
				continue

		if wait_target.empty() == False:
			if p_ret['dest_h'] == wait_target.queue[0] and p_ret['cmd'].find('state') == 0:
				#print(f'if {p_ret["dest_h"]} == {wait_target.queue[0]}')
				wait_q.put(p_ret)
				continue
		publish_status(p_ret)

#========== Main ==========

if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s[%(asctime)s]:%(message)s ', level=logging.DEBUG)

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    if config.get('RS485', 'type') == 'serial':
        import serial
        rs485 = RS485Wrapper(serial_port = config.get('RS485', 'serial_port', fallback=None))
    elif config.get('RS485', 'type') == 'socket':
        import socket
        rs485 = RS485Wrapper(socket_server = config.get('RS485', 'socket_server'), socket_port = int(config.get('RS485', 'socket_port')))
    else:
        logging.error('[CONFIG] invalid type value in [RS485]: only "serial" or "socket" is allowed')
        exit(1)
    if rs485.connect() == False:
        exit(1)

    mqttc = init_mqttc()

    msg_q = queue.Queue(BUF_SIZE)
    ack_q = queue.Queue(1)
    ack_data = []
    wait_q = queue.Queue(1)
    wait_target = queue.Queue(1)
    send_lock = threading.Lock()
    poll_timer = threading.Timer(1, poll_state)

    cache_data = []

    thread_list = []
    thread_list.append(threading.Thread(target=read_serial, name='read_serial'))
    thread_list.append(threading.Thread(target=listen_hexdata, name='listen_hexdata'))
    for thread_instance in thread_list:
        thread_instance.start()

    poll_timer.start()

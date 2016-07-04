#!/usr/bin/env python
# author: cwj
# date: 2016-02-15
# parse config file to a dictionary
# 


import ConfigParser

class CfgParser:
	def __init__(self, cfg_file = "config.cfg"):
		self.cfg_file = cfg_file

	def parse(self):
		config = ConfigParser.ConfigParser()
		config.read(self.cfg_file)

		self.conf = {}
		for section in config.sections():
			self.conf[section] = {}
			for option in config.options(section):
				if option == "port" or option == "interval":
					self.conf[section][option] = config.getint(section, option)
				else:
					self.conf[section][option] = config.get(section, option)

		return self.conf
# -*- coding: utf-8 -*-

# src/system/_ascii_plotter.py


import threading
import pprint

import sys
import textwrap
import itertools
import time

from functools import partial

import logging


# -------------------------------------------------------
# Logger initialisieren
logger = logging.getLogger(__name__)
# -------------------------------------------------------


#--------------------------------------------------------------------------------------------#

ASCII_TEXT = """

    ######## ##       ##     ## ##     ##  ######  ##     ## ######## ##     ##    #### #### #### 
    ##       ##       ##     ##  ##   ##  ##    ## ##     ## ##       ###   ###     ##   ##   ##  
    ##       ##       ##     ##   ## ##   ##       ##     ## ##       #### ####     ##   ##   ##  
    ######   ##       ##     ##    ###    ##       ######### ######   ## ### ##     ##   ##   ##  
    ##       ##       ##     ##   ## ##   ##       ##     ## ##       ##     ##     ##   ##   ##  
    ##       ##       ##     ##  ##   ##  ##    ## ##     ## ##       ##     ##     ##   ##   ##  
    ##       ########  #######  ##     ##  ######  ##     ## ######## ##     ##    #### #### ####       

"""

#--------------------------------------------------------------------------------------------#

ASCII_CYBER_PUNK = """
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
,,,,,,,,,,,,,;,,,,;,,;,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,;;;,,,;,,;;;;,,;;;;;
;;;;;;;;;;;;;;,,;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,;;;;;;;;;;;;;;
;;;;;;;;;;;;;;,',;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,',;;;;;;;;;;;;;;
:::::::;;,,;;;,'';::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::;'',;;;,,;::::::::
::::::::;''.','..,::;;;;::::::::::::::::::::::::::::::::::::::::::::::::::::;;;;::,..','.'',;:::::::
ccc:;,,'''.......',''',,;:cccccccccccccccccccccccccccccccccccccccccccccccc:;,'''','.......''',,;:ccc
cccc::;;,'..........',;:ccccccccccccccccccccclllooooollccccccccccccccccccccc:;,'..........',;;::cccc
llccccc:;,'..........'',;:ccccclcccccccccllodxxkkkkkkxxddolcclcllccccccccc:;,''..........'';:ccccccc
llllc:,'...'',,'..',;;:;;;:cllllllllllllodxkkkkkkkkkkkkkkxdollllllllllllc:;;::;;,'..',,'''..',:cllll
llc;,'...',;cc:;,..':llc:cllllllllllllldxkkkkkkkkkkkkkkkkkkxdlllllllllllllc:cll:'..,;:cc;,'...',;cll
ol:'......,;cc:c:,'.';:;,:c::clooooooodxkkkkkkkkkkkkkkkkkkkkxdooooooolc::c:,;:;'.',:c:cc;,......':lo
olcc;'.''':loc:loc,',;'..''',:cooooooodxxxxxxxxxxxxxxxxxxxxxxdoooooooc:,'''..';,',col:col:'''.';:clo
ooooc,;:c::lo::oooc:;,'.....',:loooooddxxxxxxxxxxxxxxxxxxxxxxxdoooool:,'.....',;:cooo::ol::c:;;coooo
ddddlloccoool;:oddol::cc:,',::loddddddxxxxxxxxxxxxxxxxxxxxxxxxddddddol::,.,:cc::loddd:;ooooccolldddd
dddddooccoddc,cddddlcldddc;coooddddddddxxxxxxxxxxxxxxxxxxxxxxddddddddoooc;cdddlcoddddc;cddoccodddddd
;,,;,,''';::,':lool:ldddddlodddddddddddxxxxxxxxxxxxxxxxxxxxxxdddddddddddoldddddl:cllc:',::;''',::;;:
.................'.',;::ccclloooddddddxxxxxxxxxxxxxxxxxxxxxxddddddolc:::;;;;,,,'....................
........................''''',,,;;;;::::ccccccclllllllllcccc:;;;;;,'''''''..........................
.........'''''''''''''',,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,''''''''''''''''........
.................'''''''''''''''''''''''''',''''',,''''',,'''''''''''''''''''''''''.................
..............''.''''''''''''''''','''''',,''''',,,,''''',,'''''',,'''''''''''''''''''..............
'''''',,,,,,,,,'''',,,,,,,,,,,,,,,,,,,,,,,,,,,,,,;;,,,,,,,,;,,,,,,,,,,,,,,,,,,,,,,''',,,,,,,,,,'''''
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;::;;;;;;;:::;;;;;;;;;::;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
ccc:::::::::::cccc:::::::::ccc::::::::cc::::::::cccc::::::::ccc:::::::cccc:::::::::cccc:::::::::cccc
"""

#--------------------------------------------------------------------------------------------#

ASCII_CTRL = """
                       d
                      ----
e(t)                  dt
  |                    |
  |        +---------------------+
  |        |                     |
  |    +--------+           +----------+
  +--->|  K_P   |---------->|          |
  |    +--------+           |          |
  |                         |          |
  |    +--------+           |  G(s)    |---------> y(t)
  +--->|  ∫ dt  |---------->|  Regel-  |
  |    +--------+           | strecke  |
  |                         |          |
  |    +--------+           |          |
  +--->| d/dt   |---------->|          |
       +--------+           +----------+
             ^                  ^
             |                  |
             +------------------+
"""

#--------------------------------------------------------------------------------------------#

ASCII_PENGUIN = """
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣠⣤⣤⣤⣤⣤⣤⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⣴⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣶⣤⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣼⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣼⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠿⠿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⡟⠁⠀⠀⠙⢿⣿⣿⣿⡿⠋⠀⠀⠀⠀⠙⣿⣿⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢹⡀⠀⠀⠀⠀⠈⣿⣿⣿⠁⠀⠀⠀⠀⠀⠀⠸⣿⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢨⠁⢠⣾⣶⣦⠀⢸⣿⣿⢠⣾⣿⣶⡀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⠀⢸⣿⣿⣿⠤⠘⠀⠘⠼⣿⣿⣿⡇⠀⠀⢀⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣧⡀⢹⠟⠁⠀⠀⠀⠀⠀⠈⠙⢟⣁⠀⢀⣼⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡟⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠉⠉⠻⣿⣿⣿⣿⣿⣿⣿⡄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⡆⠣⡀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣀⡤⠖⠀⠀⣠⣿⣿⣿⣿⣿⣿⣿⣧⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣼⣿⣦⡘⠢⠤⠤⠤⠤⠤⠒⠉⠁⠀⢀⣠⣴⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣼⣿⣿⠟⠉⠢⣄⣢⠐⣄⠠⣄⢢⣼⠞⠉⠀⠈⠻⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣼⣿⣿⡟⠀⠀⠀⠀⠉⠙⠚⠓⠊⠉⠀⠀⠀⠀⠀⠀⠀⢻⣿⣿⣿⣿⣿⣿⣿⣿⣿⣆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣾⣿⣿⣿⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣧⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣰⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⡀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣴⣿⣿⣿⣿⣿⡏⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⡀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣰⣿⣿⣿⣿⣿⠟⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣰⣿⣿⣿⣿⣿⠏⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣇⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣿⣿⣿⣿⣿⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢰⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣼⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢰⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣾⣿⣿⣿⣿⣿⠂⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠇⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⢰⣿⣿⣿⣿⣿⣿⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣼⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡀⠀⠀⠀⠀
⠀⠀⠀⢀⠀⠀⠀⠀⠀⢸⣿⣿⣿⣿⣿⣿⡆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⣿⣿⣿⣿⣿⣧⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⠀⠀⠀⠀
⠀⠀⠀⡰⠉⠈⠑⠠⢀⢸⣿⣿⣿⣿⣿⣿⣿⡆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠰⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡄⠀⠀⠀
⠀⠀⠀⡇⠀⠀⠀⠀⠀⠉⠙⠛⠛⠛⠿⠿⣿⣿⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠟⠉⠀⠀⠀⠘⢿⣿⣿⡇⠀⠀⠀
⠀⠀⠀⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⣧⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢻⣿⣿⣿⣿⣿⣿⣿⣿⠟⠉⠀⠀⠀⠀⠀⠀⠀⠘⣿⣿⡇⠀⠀⠀
⠀⠀⢰⠃⠀⠀⢠⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣾⠘⣿⣿⣿⣿⣿⡟⠁⠂⠀⠀⠀⠀⠀⠀⠀⠀⠀⢻⣿⡇⠀⠀⠀
⠀⡠⠊⠀⢀⠐⡀⠈⠄⠂⡐⠀⢂⠐⠈⠠⢀⠀⠀⢻⡤⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣷⠀⢀⠛⡛⢫⠑⡄⢃⡐⢈⠐⡀⠄⠐⠀⠀⠀⠀⠈⢿⡇⠂⠀⠀
⢠⢁⠀⠄⢂⡐⠠⡁⠌⡐⠠⢁⠂⠌⢠⢁⠂⡐⠀⠘⣿⣳⢤⡀⡀⢀⠀⡀⢀⠀⡀⠠⡀⠤⣁⢿⠀⠄⣂⠑⡂⠥⡘⢠⠐⢂⠰⠀⠌⡐⢈⠐⡀⠀⠀⠀⠑⢄⠀⠀
⠈⢧⡘⡐⢂⠤⠑⡠⢁⠆⡁⠆⠌⣂⠁⡂⠌⡐⠀⠀⢹⣿⣷⣧⣝⣢⠱⡰⣈⢆⢡⢃⠴⡱⣌⣾⠈⡐⢠⠘⡠⢁⠆⡡⢘⠠⡁⠎⡐⡈⢄⠢⢀⠡⢀⠈⠂⠀⠑⡀
⠀⠀⠙⢵⣊⠴⡁⢆⠡⢂⠅⡊⠔⡠⠘⢄⠒⡀⢁⠀⠀⢻⣿⣿⣿⣿⣿⣷⣷⣾⣶⣿⣾⣿⣿⣿⠀⠐⡄⠢⢁⠆⡘⢄⠡⢂⠱⢠⠑⡨⢄⠢⣁⠒⡄⢊⠄⣂⢀⡡
⠀⠀⠀⠀⠉⠲⣍⢢⠱⡈⢆⠱⡈⠔⡉⢄⠒⠄⢂⠀⢀⠀⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡟⠀⠂⢄⠣⠌⣂⠱⡈⢆⠡⢊⠄⢣⠐⢢⠑⡄⢣⡘⢆⡳⣬⠞⠁
⠀⠀⠀⢀⠀⠀⠈⢣⡞⡰⢈⠆⡱⢈⠔⡨⢘⡈⠆⢌⠀⡐⠨⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠠⢉⠄⢢⠑⡄⢣⠘⡄⠣⢌⢊⡔⡉⢦⠩⡜⣡⢞⡷⠋⠁⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠙⢶⡉⢆⡱⢈⠆⡑⠢⢌⡘⢄⠣⡐⣡⠏⠉⠉⠉⠉⠉⠉⠉⠉⠍⠉⠉⢳⢁⠊⡜⢠⠃⡜⢠⠃⣌⠱⣈⠦⢰⡉⢆⡳⣼⠟⠁⠀⠀⡆⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠄⠀⠀⠹⣖⡰⢃⡜⢄⠳⣠⠚⣌⠖⣥⡿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⣎⠴⡈⢆⠱⡈⢆⠱⡠⢃⠖⣌⠣⣜⢣⠟⠁⠀⠀⠀⠀⠃⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠓⢯⡼⣬⣓⣦⣟⣼⡿⠚⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⣶⣉⢆⠳⡌⡜⢢⠱⡩⢜⣤⢻⡼⠋⠀⠀⢸⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠐⠀⠀⠀⠉⠙⠛⠛⠋⠉⠀⡀⠀⠀⠐⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠛⢾⣳⣼⣜⣧⣳⡽⣞⠞⠋⠀⠀⠀⠀⠒⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠀⠀⠀⠀⠈⠉⣉⢉⣉⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
"""

#--------------------------------------------------------------------------------------------#

METRICS_LEGENDE = """ 
    Legende – Queue‑Metriken (Einheit: Millisekunden; gleitende Fenster ~1 s / ~60 s)

    Spalten:
      Queue    : Klarname der Queue (falls registriert), sonst generische Objektkennung "Queue#<id>".
      size     : Aktuelle Queue‑Länge (qsize). Ziel im Normalbetrieb: nahe 0. Bursts ok, sollten rasch abklingen.
      inflt    : "In‑Flight" im Messfenster – näherungsweise offene Elemente (≈ max(0, puts - gets)).
                 Bleibt inflt > 0 bestehen, holt der/die Konsument(en) nicht auf → Backpressure.

      put/s    : Enqueue‑Rate pro Sekunde (kurzes Fenster).
      get/s    : Dequeue‑Rate pro Sekunde (kurzes Fenster).

      put/min  : Enqueues pro Minute (längeres Fenster, geglättet).
      get/min  : Dequeues pro Minute (längeres Fenster, geglättet).

      lat_avg  : Durchschnittliche End‑to‑End‑Queue‑Latenz = Zeit zwischen put() und dem dazugehörigen get().
                 Das ist nur die Wartezeit in der Queue – *nicht* die Downstream‑Verarbeitung nach dem get().

      p95      : 95. Perzentil der End‑to‑End‑Latenz (Tail‑Latency).
      p99      : 99. Perzentil (Tail‑Latency, zeigt Ausreißer/Spitzen deutlicher an).
      max      : Maximale beobachtete End‑to‑End‑Latenz im Aggregatfenster.

      put_rt   : Durchschnittliche Dauer eines einzelnen put() (Serialisierung + Enqueue + Executor‑Hop).
      get_rt   : Durchschnittliche Dauer eines einzelnen get() (Dequeue + Deserialisierung).
                 Ist die Queue leer, enthält get_rt die Wartezeit bis zur Verfügbarkeit (Blockieren/Timeout). 
                 Hohe Werte bei inaktiven Queues sind normal.

      err      : Fehlerzähler im Fenster als "put_err/get_err/timeout".
                 - 'timeout' steigt bei leeren Queues (get‑Timeouts) und ist in Leerlauf‑Phasen erwartbar.
                 - 'put_err' oder 'get_err' > 0 sind relevant/kritisch.

    Typische Richtwerte (Single‑Prozess, In‑Memory):
      • size / inflt  : im Mittel ~0; Peaks ok, sollten binnen Sekunden/Minuten auf ~0 zurückgehen.
      • lat_avg       : <10–30 ms typisch; p95 <100–200 ms. Einzelne Spitzen sind ok.
      • p99           : möglichst nahe p95; dauerhaft > 500–1000 ms = Warnzeichen.
      • put_rt/get_rt : i. d. R. <1 ms; bei Leerlauf steigt get_rt durch Warten (kein Fehler).

    Gesundheitssignale (heuristisch):
      • Dauerhaft inflt > 0 *und* get/s < put/s → Backpressure.
      • p95/p99 ~ Sekundenbereich ohne Leerlauf → Bearbeitung nach get() blockiert/zu langsam.
"""

#--------------------------------------------------------------------------------------------#

def print_ascii_art(ascii_string, output_stream=sys.stdout, indent=0, rgb=None):
    """Generische Funktion zum Drucken von ASCII-Art mit optionaler Einrückung und RGB-Farbe.
    
    rgb kann sein:
    - (r, g, b): komplette Zeilen in einer Farbe
    - Liste/Tuple von (r, g, b): pro Zeichen zyklische Palette (Cyberpunk-Regenbogen)
    - dict {char: (r, g, b)}: zeichenbasierte Palette (z.B. Palmen / Himmel / Boden getrennt)
    """
    # Normalisiere die Einrückung
    dedented = textwrap.dedent(ascii_string).strip()
    lines = dedented.split('\n')

    # Writer
    print_line = partial(output_stream.write)

    # Einrückung
    indent_str = '\t' * indent

    # --- Modus-Erkennung -------------------------------------------------
    # char-map-Modus: dict {'c': (r,g,b), ...}
    char_map_mode = isinstance(rgb, dict)

    # Palette-Modus: Liste/Tuple von (r,g,b)
    palette_mode = (
        not char_map_mode
        and isinstance(rgb, (list, tuple))
        and len(rgb) > 0
        and all(isinstance(c, tuple) and len(c) == 3 for c in rgb)
        and not (isinstance(rgb, tuple) and len(rgb) == 3 and isinstance(rgb[0], int))
    )

    # Einzel-Farbmodus: (r,g,b)
    single_color_mode = (
        not char_map_mode
        and not palette_mode
        and isinstance(rgb, tuple)
        and len(rgb) == 3
        and all(isinstance(v, int) for v in rgb)
    )

    color_start = ''
    color_end = ''

    if single_color_mode:
        r, g, b = rgb
        color_start = f'\033[38;2;{r};{g};{b}m'
        color_end = '\033[0m'

    # --- Rendering -------------------------------------------------------
    for line in lines:
        if char_map_mode:
            # Zeichen-spezifische Cyberpunk-Palette
            colored_chars = []
            for ch in line:
                if ch in rgb:
                    r, g, b = rgb[ch]
                    colored_chars.append(f'\033[38;2;{r};{g};{b}m{ch}')
                else:
                    # Fallback: unverändert
                    colored_chars.append(ch)
            colored_line = ''.join(colored_chars) + '\033[0m'
        elif palette_mode:
            # Zyklische Palette pro Zeichen
            colored_chars = []
            for idx, ch in enumerate(line):
                r, g, b = rgb[idx % len(rgb)]
                colored_chars.append(f'\033[38;2;{r};{g};{b}m{ch}')
            colored_line = ''.join(colored_chars) + '\033[0m'
        else:
            # Klassischer Modus: eine Farbe oder gar keine
            colored_line = color_start + line + color_end

        print_line(indent_str + colored_line + '\n')

    output_stream.flush()

# Cyberpunk-Palette: Neon-Cyan, Neon-Magenta, Neon-Gelb, Neon-Türkis
CYBER_TEXT_PALETTE = [
    (255,  64, 160),   # Neon-Pink
    (200,   0, 255),   # Violettes Neon
    (255,  96, 220),   # Soft-Pink
    (160,   0, 200),   # Tiefes Magenta
]


CYBERPALM_PALETTE = {
    # --- Himmel / Hintergrund (dunkles Violett → Magenta) ---
    ',': (20,   0,  36),   # sehr dunkles Violett (oberer Himmel)
    ';': (40,   0,  70),   # dunkles Violett / Purple
    ':': (80,   0, 120),   # Übergang zu Magenta

    # --- Glow / Sterne / Horizont-Glühen (Pink / Magenta) ---
    '.': (255,   0, 168),  # kräftiges Magenta-Glühen
    "'": (255,  74, 194),  # Neon-Pink (Highlights)

    # --- Sonne (hier bewusst GELB/ORANGE, nur hier) ---
    'o': (255, 200,  87),  # warmes Gelb (oberer Sonnenbereich)
    '0': (255, 160,  66),  # Orange-Gelb (unterer/innerer Sonnenbereich)

    # --- Palmen-Silhouetten (violette Schatten, leicht angehoben) ---
    'c': (90,  30, 140),   # violetter Palmblatt-Ton
    'l': (110, 50, 170),   # etwas heller, mit minimalem Grünanteil
    'd': (70,  20, 110),   # dunkler Stamm / Insel-Kontur

    # --- Grid / Wasser (Magenta + etwas Blau) ---
    'x': (60, 150, 255),   # blau-violettes Neon (Grid-Linien / Wasser)
    'k': (210,  0, 180),   # magentafarbene Grid-Linien
}


"""

# Schriftzug "ASCII_TEXT" im Cyberpunk-Style (pro Zeichen wechselnde Farbe)
print('\n' '\n' '\n')
print_ascii_art(ASCII_TEXT, indent=3, rgb=CYBER_TEXT_PALETTE)
print('\n' '\n' '\n')

time.sleep(2)
print_ascii_art(ASCII_CYBER_PUNK, indent=3, rgb=CYBERPALM_PALETTE)
print('\n' '\n' '\n')

time.sleep(2)
print('\n' '\n' '\n')

print_ascii_art(ASCII_CTRL, indent=3)

print('\n' '\n' '\n')
time.sleep(2)

print('\n' '\n' '\n')
print_ascii_art(ASCII_PENGUIN, indent=9)            # bleibt Standardfarbe
print('\n' '\n' '\n')

time.sleep(2)

print('\n' '\n' '\n')
print_ascii_art(METRICS_LEGENDE, indent=3)          # bleibt Standardfarbe
print('\n' '\n' '\n')
time.sleep(5)

"""

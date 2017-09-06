# coding: utf-8

import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import re
import itchat
import time
import datetime


urls = {
    'fuyou' : 'http://www.bjguahao.gov.cn/dpt/appoint/12-200004194.htm',
    'renmin' : 'http://www.bjguahao.gov.cn/dpt/appoint/120-200000930.htm',
    'fyweixin': 'http://wx.ezhuanzhen.com/beida1_wx/hyzslist.php?keid=L[[)]]~Itp8G'
}
# url = 'http://www.bjguahao.gov.cn/dpt/appoint/12-200004196.htm'
headers = {
    'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'
    # 'User-Agent': 'mozilla/5.0 (linux; u; android 4.1.2; zh-cn; mi-one plus build/jzo54k) applewebkit/534.30 (khtml, like gecko) version/4.0 mobile safari/534.30 micromessenger/5.0.1.352'
    # 'User-Agent': 'mozilla/5.0 (iphone; cpu iphone os 5_1_1 like mac os x) applewebkit/534.46 (khtml, like gecko) mobile/9b206 micromessenger/5.0'
}
app_stop = True

def check(url):
    r = requests.get(url, headers = headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    # print soup.title
    tds = soup.find_all('td')
    # print re.findall(r'<td>(.*?)</td>', r.text, re.M|re.I|re.S)
    state = [re.sub(r'\s', '', td.text) for td in tds[:14]]

    # print state
    for s in state:
        if s == u'\xa0':
            continue
        if s == u'\u7ea6\u6ee1':
            continue
        return 1
    return 0


def check2(url, match_date=None):
    r = requests.get(url, headers = headers)
    r.encoding = 'gb2312' # fyweixin 网页编码无法识别需要手动指定
    soup = BeautifulSoup(r.text, 'html.parser')
    # print soup.title
    # tds = soup.find_all('td')
    data = soup.select('div.block12 a')[1:]
    for a in data:
        clas = a.div['class'][0]
        if clas != 'block12a':
            # break
            continue
            # pass
        date = a.div.div.text
        href = a['href']
        if (not match_date) or date.startswith(match_date):
            print date
            return 1
    return 0

def bypass():
    u = itchat.search_friends(name='me')[0]
    while 1:
        # if app_stop:
        #     time.sleep(5)
        timestr = str(datetime.datetime.now())[:19]
        # free = check(urls['fuyou'])
        free = check2(urls['fyweixin'], u'2017-09-07 周四 上午')
        # free = 1
        if free:
            print timestr + ': sending!'
            u.send_msg(u'现在有号，请快到网站预约！')
            raw_input()
            # app_stop = True
        else:
            print timestr + ': sell out'
        time.sleep(5)

@itchat.msg_register(itchat.content.TEXT)
def chat_trigger(msg):
    # return msg['Text']
    text = msg['Text']
    if text == u'帮助' or text == 'help':
        return u'开始/start\n停止/stop'
    elif text == u'开始' or text == 'start':
        app_stop = False
    elif text == u'停止' or text == 'stop':
        app_stop = True
    else:
        return u'错误命令'
itchat.run()


if __name__ == '__main__':
    # itchat.auto_login()
    # itchat.auto_login(enableCmdQR=2)  # 命令行下
    itchat.auto_login(hotReload=True, enableCmdQR=2)   # 一段时间不用扫码     只要这行
    # itchat.send('Hello, filehelper', toUserName='filehelper')
    # itchat.send('Hello, filehelper', toUserName=u'@906d2590fc473f7169e279fdb6f0667b')
    
    # itchat.run()
    bypass()


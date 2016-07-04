#!/usr/bin/env python

'''send product key metrics report to mail.vobile.cn mailbox'''
from email.header import Header
import sys
import os,time
import mail
import getopt

report_time =time.strftime('%Y-%m-%d',time.localtime())
report_start=time.strftime('%Y-%m-%d',time.localtime(time.time()-86400*7))
report_end=time.strftime('%Y-%m-%d',time.localtime(time.time()-86400*1))


email_list = ['wang_geng@vobile.cn','sun_cong@vobile.cn'] 
email_list=', '.join(email_list)
print email_list

def send_plain(subject, body):
    '''Send Subject and body'''
    plain_text = mail.plain(text=body, charset='utf-8')
    prepared_msg = mail.prepare(message=plain_text, \
            from_name='mailserver_monitor', \
            from_email='mailserver_monitor@vobile.cn', \
            to=email_list, \
            subject=Header(subject, 'utf-8')),\
    mail.send_smtp(host='mail.vobile.cn', \
            username='mailserver_monitor@vobile.cn', \
            password='mailserver_monitor', \
            message=prepared_msg, \
            verbose=False,\
            starttls = True)
    return

def send_attachment(subject, attachment):
    '''Send Subject and body'''
    part = mail.part(filename=attachment, content_type='binary/plain')
    plain_text = mail.multi(parts=[part])
    prepared_msg = mail.prepare(message=plain_text, \
            from_name='mailserver_monitor', \
            from_email='mailserver_monitor@vobile.cn', \
            to=email_list, \
            subject=Header(subject, 'utf-8'))
    mail.send_smtp(host='mail.vobile.cn', \
            username='mailserver_monitor@vobile.cn', \
            password='mailserver_monitor', \
            message=prepared_msg, \
            verbose=False,\
            starttls = True)
    return

def get_email_list():
    i = 0
    while i < len(email_list):
        email_list[i] = email_list[i].strip()
        i += 1
    
    print email_list

def main():
    '''Main function'''
    options, _ = getopt.gnu_getopt(sys.argv[1:], "s:b:a:", ["subject=", "body=", "attachment="])
    attachment = None
    for i in options:
        if i[0] == "--subject" or i[0] == "-s":
            subject = i[1]
        if i[0] == "--body" or i[0] == "-b":
            body = i[1]
        if i[0] == "--attachment" or i[0] == "-a":
            attachment = i[1]
     
    if attachment:
        send_attachment(subject, attachment)
    else:
        send_plain(subject, body)

if __name__ == "__main__":
    try:
        main()
    except Exception, ex:
        print ex

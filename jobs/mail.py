#!/usr/bin/env python 

from boto.ses.connection import SESConnection

FOOTER = """
<p>Sentimentron has removed your email from its database.<br />
--<br />
Sentimentron is a research project, results are automatically generated and may not be accurate. 
<a href=\"http://www.sentimentron.co.uk/info.html#terms\">Terms of use</a>"""

class EmailProcessor(object):

    def __init__(self):
        self.con = SESConnection()

    def send_success(self, to, id):
        msg = """<p>Hello! This is an automated email from Sentimentron.</p>

<p>Sentimentron's finished processing your request and has produced some results. 
To view them, copy the following into your browser:

    <ul style="list-style-type:none"><li><a href="http://perma.sentimentron.co.uk/%d">http://perma.sentimentron.co.uk/%d</a></li></ul>

Hope you find the results useful! If you have any questions or feedback, please email <a href="mailto:feeback@sentimentron.co.uk">feedback@sentimentron.co.uk</a></p>.""" % (id, id)
    
        self.con.send_email("no-reply@sentimentron.co.uk", 
            "Good news from Sentimentron",
            msg + FOOTER,
            [to], None, None, 'html'
        )

    def send_failure(self, to, error):
        msg = """<p>Hello! This is an automated email from Sentimentron.</p>

<p>Unfortunately, Sentimentron encountered a problem processing your query and
hasn't produced any results. Apologies for the inconvenience. The problem was:

    <ul style="list-style-type:none; font-weight:bold"><li>%s</li></ul>

If you have any questions or feedback, please email <a href="mailto:feeback@sentimentron.co.uk">feedback@sentimentron.co.uk</a>.</p>""" % (error,)

        self.con.send_email('no-reply@sentimentron.co.uk', 
            'Bad news from Sentimentron',
            msg + FOOTER,
            [to], None, None, 'html'
        )

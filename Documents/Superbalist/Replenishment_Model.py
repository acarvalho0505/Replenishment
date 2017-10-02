
# coding: utf-8

# In[1]:


#Let's get jiggy with it
import pandas as pd
import numpy as np
import time
import datetime as dt
from datetime import datetime, timedelta

#mysql import
import pymysql
host = '10.0.28.22'
con = pymysql.connect(host, "qlikview", "F3WxH7AkJP85", "superbalist")


# In[2]:


q = """
Select s.id as 'sku_id', s.product_id,  departments.id as 'department_id', departments.name as 'department_name', categories.name as 'category_name', designers.name as "Brand", p.name as 'product_name', size_template_values.value as 'size_value', s.retail_price as 'RSP', product_markdowns.new_price as 'new_price', s.physical_stock_qty as todays_stock, s.status, p.publish_date as 'publish_date', CASE WHEN s.status = "SOLD OUT" THEN max(from_unixtime(orders.created_at)) END as "final_sale_date"
FROM skus as s
	left join products as p on s.product_id = p.id
	left join products__categories on s.product_id = products__categories.product_id
	inner join categories on categories.id = products__categories.category_id
	inner join departments on departments.id = categories.department_id
	left join size_template_values on s.size_template_value_id = size_template_values.id 
	left join product_markdowns on p.id = product_markdowns.product_id 
	left join designers on p.designer_id = designers.id 
	inner join order_items on  s.id = order_items.sku_id
	inner join orders on orders.id = order_items.order_id
	where p.publish_date >= '2016-01-01'
	group by s.id
    ;
"""


# In[3]:


# Using the sql code (q) and the connection through pymysql (con) to set orders equal to our dataframe
replen = pd.read_sql(q, con)


# In[4]:


#Adding the Markdown%
replen["Markdown%"] = (1 - (replen["new_price"]/replen["RSP"])) 


# In[5]:


#Changing 'final_sale_date' to datetime then date
replen['final_sale_date'] = pd.to_datetime(replen['final_sale_date'])
replen['final_sale_date'] = replen['final_sale_date'].dt.date


# In[6]:


#Changing 'publish_date' to datetime 
replen['publish_date'] = pd.to_datetime(replen['publish_date'])
replen['publish_date'] = replen['publish_date'].dt.date


# In[7]:


#Adding 'Product_Age' column
replen['product_age_weeks'] = dt.datetime.now().date() - replen["publish_date"]
replen['product_age_weeks'] = replen['product_age_weeks'] / np.timedelta64(1, 'W')


# In[8]:


#Adding the weeks out of stock
replen['days_outta_stock'] = (dt.datetime.now().date() - replen['final_sale_date'])
replen['days_outta_stock'] = (replen['days_outta_stock'] / np.timedelta64(1, 'W'))
replen=replen.rename(columns = {'days_outta_stock':'weeks_out_of_stock'})


# In[9]:


#getting the orginal stock info
q2 = """
Select bri.sku_id, bri.quantity as "quantity_originally_purchased", max(FROM_UNIXTIME(bri.created_at)) as "last_buy_request"
From buy_request_items as bri
group by bri.sku_id;
"""


# In[10]:


o_stock = pd.read_sql(q2, con)


# In[11]:


#Changing 'last_buy_request' to datetime then date
o_stock['last_buy_request'] = pd.to_datetime(o_stock['last_buy_request'])


# In[12]:


#merging o_stock to replen
replen =pd.merge(replen, o_stock, how='inner', on = 'sku_id')


# In[13]:


#calculating current_sell_thru%
replen["current_sell_thru%"] = ((replen["quantity_originally_purchased"] - replen["todays_stock"])/replen["quantity_originally_purchased"]) 
replen['current_sell_thru%'] = pd.Series(["{0:.2f}%".format(val * 100) for val in replen['current_sell_thru%']]) #changing sell through into %


# In[14]:


#quering last_4_weeks_sales
q3 = """
Select oi.sku_id, count(*) as last_4_weeks_sales
FROM order_items as oi
WHERE FROM_UNIXTIME(oi.created_at) >= current_date - interval '28' day
Group by oi.sku_id;
"""


# In[15]:


last_4weeks_sales = pd.read_sql(q3, con)


# In[16]:


#quering first_4_weeks_sales
q4 = """
Select oi.sku_id, count(*) as first_4_weeks_sales
FROM order_items as oi
inner join skus as s on s.id = oi.sku_id
inner join products as p on p.id = s.product_id 
WHERE FROM_UNIXTIME(oi.created_at) >= p.publish_date + interval '28' day
AND p.publish_date >= "2016-01-01"
Group by oi.sku_id;
"""


# In[17]:


first_4weeks_sales = pd.read_sql(q4, con)


# In[18]:


#merging last_4weeks_sales to replen
replen = pd.merge(replen, last_4weeks_sales, how = 'left', on = 'sku_id')


# In[19]:


#merging first_4weeks_sales to replen
replen = pd.merge(replen, first_4weeks_sales, how = 'inner', on = 'sku_id')


# In[20]:


#Quering the total sold for each item
q5 = """
Select oi.sku_id, count(*) as 'total_sold'
FROM order_items as oi
inner join skus as s on s.id = oi.sku_id
inner join products as p on p.id = s.product_id
where publish_date >= '2016-01-01'
group by sku_id;
"""


# In[21]:


totals = pd.read_sql(q5, con)


# In[22]:


replen = pd.merge(replen, totals, how = 'inner', on = 'sku_id')


# In[23]:


#Only taking the products that have a buy request over the past 6 months
replen = replen.set_index("last_buy_request").last("6M") #can change int in the .last to change the past month range
replen = replen.reset_index(level=['last_buy_request'])
replen['last_buy_request'] = replen['last_buy_request'].dt.date


# In[24]:


#chaning variables to integers and filling na's
replen["last_4_weeks_sales"] = replen["last_4_weeks_sales"].fillna(0)
replen['last_4_weeks_sales'] = replen['last_4_weeks_sales'].astype(int)
replen['first_4_weeks_sales'] = replen['first_4_weeks_sales'].astype(int)


# In[25]:


#changing back to datetime for for loop
replen['final_sale_date'] = pd.to_datetime(replen['final_sale_date'])
replen['publish_date'] = pd.to_datetime(replen['publish_date'])


# In[26]:


#creating empty column
replen['ATB'] = np.nan


# In[27]:


#ATB
for i in range(0,len(replen)):
    if(replen["status"][i] == "SOLD OUT"):
        replen["ATB"][i] = replen["final_sale_date"][i] - replen["publish_date"][i]
    else:
        replen["ATB"][i] = dt.datetime.now() - replen["publish_date"][i]


# In[28]:


#chaning the ATB to an int
replen['ATB'] = (replen['ATB'] / np.timedelta64(1, 'D')).astype(int)


# In[29]:


#calculating the weekly average sold
replen['weekly_avg_sales'] = (replen['total_sold']/((replen['ATB'])/7))


# In[30]:


#Changing ATB days to ATB Weeks
replen['ATB'] = replen['ATB']/7
replen=replen.rename(columns = {'ATB':'weeks_ATB'})


# In[31]:


#Creating week_of_stock
replen["weeks_of_stock"] = replen["todays_stock"][i]/replen["weekly_avg_sales"]


# In[32]:


#adding supplier information
q6= """
 Select bri.sku_id, s.id as supplier_id, s.name as supplier
from buy_request_items as bri
    inner join buy_requests as br on br.id = bri.buy_request_id
    inner join suppliers as s on s.id = br.supplier_id
    order by bri.sku_id;
    """


# In[33]:


suppliers = pd.read_sql(q6, con)


# In[34]:


#merging replen to suppliers
replen = pd.merge(replen, suppliers, how = 'inner', on = 'sku_id')


# In[35]:


#adding lead time based on supplier averages
q7 = """
Select l.supplier_id, AVG(l.lead_time_days) as lead_time_days
From(
select q.sku_id, q.supplier_id, q.supplier, (q.arrival_date - q.buy_request_created) as lead_time_days
from(
select bri.sku_id, s.id as supplier_id, s.name as supplier, Convert(from_unixtime(br.created_at), date) as buy_request_created, so.arrival_date
from buy_request_items as bri
    inner join buy_requests as br on br.id = bri.buy_request_id
    inner join suppliers as s on s.id = br.supplier_id
    inner join sales_order_requests as sor on sor.buy_request_id = br.id
    inner join sales_orders as so on so.id = sor.sales_order_id
    where br.status= "APPROVED") q) l
group by l.supplier_id;
"""


# In[36]:


lead = pd.read_sql(q7, con)


# In[37]:


#adding on lead times to replen
replen = pd.merge(replen, lead, how = 'left', on = 'supplier_id')


# In[38]:


#Adding lead_time_weeks column
replen["lead_time_weeks"] = replen["lead_time_days"]/7


# In[39]:


#Calculating current orders placed
q8 = """
Select sori.sku_id, sori.quantity as units_on_order, so.estimated_arrival_date
From sales_order_request_items as sori
inner join sales_order_requests as sor on sor.id = sori.sales_order_request_id
inner join sales_orders as so on so.id = sor.sales_order_id
where so.arrival_date is NULL
AND so.estimated_arrival_date >= CURDATE()
group by sku_id;
"""


# In[40]:


orders = pd.read_sql(q8, con)


# In[41]:


#merging orders onto replen
replen = pd.merge(replen, orders, how = "left", on = "sku_id")


# In[42]:


#making on_orders_@RSP column
replen["on_order_@RSP"] = replen["units_on_order"]*replen["RSP"]


# In[43]:


#making weeks_stock_minus_lead
replen["weeks_stock_minus_lead"] = replen["weeks_of_stock"] - replen["lead_time_weeks"]


# In[44]:


#dropping extra columns and cleaning table
replen.drop(['last_buy_request','publish_date', 'supplier', 'supplier_id', 'total_sold', 'final_sale_date'], axis=1, inplace=True)


# In[45]:


#changing the order of the columns
replen = replen[['department_id', 'department_name', 'category_name','Brand',
       'product_name', 'product_id', 'sku_id', 'size_value', 'RSP', 'new_price',
       'Markdown%', 'lead_time_days', 'status', 'product_age_weeks', 'weeks_ATB', 'first_4_weeks_sales', 
        'last_4_weeks_sales', 'weekly_avg_sales', 'quantity_originally_purchased', 'current_sell_thru%',
        'todays_stock', 'weeks_of_stock', 'weeks_out_of_stock', 'units_on_order',
       'on_order_@RSP', 'estimated_arrival_date', 'lead_time_weeks',
       'weeks_stock_minus_lead']]


# In[46]:


replen = replen.fillna('')


# In[47]:


replen = replen.sort_values(by=['last_4_weeks_sales'], ascending=[False])


# In[48]:


#Setting up email
import email
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders
import os

import smtplib


# In[49]:


replen.to_excel("replenishment_sheet " +datetime.today().strftime("%d-%m-%Y")+ ".xlsx", index=False)


# In[50]:


smtpUser = 'ds1superbalist@gmail.com' #email of sender
smtpPass = 'datascience' #password of sender

toAdd = ['alex.c1258@gmail.com', 'amy.m@superbalist.com'] #recipient
fromAdd = smtpUser

today = datetime.today()

subject  = 'Automated replenishment email test %s' % today.strftime('%Y %b %d')
header =  'From : ' + fromAdd + '\n' + 'Subject : ' + subject + '\n'
body = 'This is a test for the automated email that will go out each day at a specific time containing the list of possible replenishment items. It only includes items that have a buy request within the past 6 months. Please let me know that you have received it and let me know if there is anything you would like changed. Additionally please let me know who must receive the email and at what time it should be sent out. '

attach = "replenishment_sheet " +datetime.today().strftime("%d-%m-%Y")+ ".xlsx"  #file to attach

print (header)


def sendMail(to, subject, text, files=[]):
    assert type(to)==list
    assert type(files)==list

    msg = MIMEMultipart()
    msg['From'] = smtpUser
    msg['To'] = COMMASPACE.join(to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach( MIMEText(text) )

    for file in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(file,"rb").read() )
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"'
                       % os.path.basename(file))
        msg.attach(part)

    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo_or_helo_if_needed()
    server.starttls()
    server.ehlo_or_helo_if_needed()
    server.login(smtpUser,smtpPass)
    server.sendmail(smtpUser, to, msg.as_string())

    print ('Done')

    server.quit()


sendMail( toAdd, subject, body, [attach] )


# In[51]:


#delete file from server
os.remove("replenishment_sheet " +datetime.today().strftime("%d-%m-%Y")+ ".xlsx")


# In[ ]:





import pymysql
import numpy as np
import pandas as pd
import boto3
import psycopg2


def dbconnection():

    '''Database connection block
    Access the S3 buckets to get the credentials to access RDS
    Establish a connection to the AWS RDS Mysql
    :return: connection
    '''

    # Code to access S3 bucket
    # s3 = boto3.client('s3')
    # obj = s3.get_object(Bucket='bizapps.uaudio', Key='redshift.env.sh')
    # content = (obj['Body'].read())
    # env_var = content.decode('ascii')
    # print(env_var)

    hostname = '**************'
    #  has to be removed once the S3 credentials bucket is setup and test to access the credentials directly from S3
    username = 'redshift'
    psd = '**************'

    conn = pymysql.connect(
        host=hostname,
        user=username,
        password=psd,
        cursorclass=pymysql.cursors.DictCursor
    )

    conn1 = psycopg2.connect(
        dbname= 'uaudio',
        host='bizappsredshift.cmlafnptibhc.us-west-2.redshift.amazonaws.com',
        port= '5439',
        user= 'uadbadmin',
        password= 'uWb6~ha-{mbi')

    return conn, conn1


def buildskumap():
    ''' SKUMAP block
    Builds the SKU MAP from SKUS, SKUS_PRODUCTS and PRODUCTS
    SKU MAP is a dictionary with  key(K), value(V) pairs (SKU Name(K): ProductCodes(V)
    The SKU MAP is sorted based on the count of produtcode. SKU with most number of product will be in the top of the map
    :return: newskumap -> {SKU name : List of Products}
    '''

    # Query to build the SKU map from RDS
    with conn.cursor() as cursor:
        sql = """select  REPLACE(sp.skus_id, IF(sp.skus_id LIKE 'UAD-2%', 'UAD-2','UAD-1'), 'UAD') AS sku_id,
                    GROUP_CONCAT(SUBSTR(sp.products_code, 3) ORDER BY sp.products_code) AS product_codes
                 from uaudio.skus s
                 join uaudio.skus_products AS sp  ON s.skus_id=sp.skus_id
                 join uaudio.products AS p ON sp.products_code=p.products_code
                 where s.skus_class_id IN ('UAD-2-PAID','UAD-1-PAID')
                 AND p.products_class_id IN ('UAD-2-SW','UAD-1-SW')                 
                 AND s.skus_status='Active'
                 AND s.skus_product_type='plugin'
                 AND s.skus_bundle=0
                 GROUP BY sp.skus_id"""
        cursor.execute(sql)
        sku = cursor.fetchall()
        skumap={}
        for i in range(len(sku)):
            skumap[sku[i]['sku_id']] = sku[i]['product_codes']

        # Sorting logic
        revskumap = {v: k for k, v in skumap.items()}
        # skumap = (sorted(skumap.values(), key=lambda kv: kv[1]))
        skumap = (sorted(skumap.values(), key=lambda kv: -len(kv.split(','))))
        newskumap = {}
        for ar in skumap:
            newskumap[revskumap[ar]] = ar
        return newskumap


def getorders():
    ''' Fetch Orders Block
    Start with the vouchers database to tie to the orders and get all matching voucher serial and orders
    The block will handle to compare to the new database and eliminate the orders which has been already processed
    :return: {voucher_serial,order_id}
    '''

    # Query to fetch voucher_serial and order_id pairs
    # Changes - remove data / voucher_type
    # state/status - one of them has to be updated in the query

    with conn.cursor() as cursor:
        sql = """select distinct v.vouchers_serial, v.voucher_type, o.entity_id, i.sku, i.item_id, o.customer_id, i.created_at
                    from uaudio.vouchers v 
                    JOIN uaudio.sales_flat_order o 
                    ON v.vouchers_purchase_ordernum = o.entity_id
                    JOIN uaudio.sales_flat_order_item i
                    ON i.vouchers_serial = v.vouchers_serial
                    where v.voucher_type = 'purchase' 
                    # AND vouchers_purchase_date BETWEEN '2018-06-01' AND '2018-06-30' 
                    AND o.entity_id = 1192049
                    AND o.state = 'complete' AND status = 'complete'
                    """
        cursor.execute(sql)
        result = cursor.fetchall()
        return result


def processorders(vouchers, conn1):
    SkuMap = buildskumap()
    print('SKUMAP:')
    print(SkuMap)

    product_catalog= catalogproducts()

    for order in vouchers:
        if order['voucher_type'] == 'purchase':
            iscustom = customrorders(order['entity_id'])
            if (iscustom and order['sku'][:10] == 'UAD-CUSTOM'):
                customorderrecord(order, product_catalog, conn1)
                buildcustomdata(order, product_catalog, conn1)

            else:
                vouc = getproductcodes(order['vouchers_serial'], order['voucher_type'], order['entity_id'], order['sku'], order['item_id'], order['customer_id'], order['created_at'])
                orderitem = getskusforprodcodes(vouc, SkuMap)
                builddata(orderitem, product_catalog, conn1)
        else:
            print("Not Store")
            print(order)


def getproductcodes(vouchers, voucher_type, orderid, ordersku, itemid, customerid, createdat):
    '''
    Using voucher serial, list of product codes associated is retrieved
    :param vouchers:
    :param orderid:
    :param ordersku:
    :param itemid:
    :param customerid:
    :param createdat:
    :return:
    '''
    with conn.cursor() as cursor:
        sql = """SELECT vp.products_code
                    FROM uaudio.vouchers_products vp 
                    LEFT OUTER JOIN uaudio.customers_products_sw cps 
                    ON(vp.vouchers_serial = cps.vouchers_serial 
                    AND vp.products_code = cps.products_code) 
                    WHERE vp.vouchers_serial = %s"""
        cursor.execute(sql, (vouchers, ))
        result = cursor.fetchall()
        products = []
        vouc={}
        for product in result:
            # products.append(int(product['products_code']))
            products.append(product['products_code'])


        ####Test to eliminate pre owned products from the list of products
        # ownedprods = ownedproducts(orderitem={'customer_id': customerid, 'voucherserial': vouchers, 'created_at': createdat})
        # products = [x for x in products if x not in ownedprods]
        ####End

        vouc['orderid'] = orderid
        vouc['item_id'] = itemid
        vouc['customer_id'] = customerid
        vouc['ordersku'] = ordersku
        vouc['voucherserial'] = vouchers
        vouc['voucher_type'] = voucher_type
        vouc['created_at'] = createdat
        vouc['prodcodes'] = products
    return vouc


def getskusforprodcodes(vouc, SkuMap):
    '''
    Using the list of product codes associated to a voucher, ASP Skus are built
    :param vouc:
    :param SkuMap:
    :return:
    '''
    # vouc has {'orderid': , 'voucherserial': , 'prodcodes': }
    skus=[]
    for prodcode in vouc['prodcodes']:
        prodcode = str(prodcode[-5:])
        for key, value in SkuMap.items():
            if prodcode == value:
                skus.append(key)
    vouc['SKUs'] = skus
    prodcodes=[]
    for prodcode in vouc['prodcodes']:
        prodcodes.append(prodcode[-5:])

    # print(SkuMap)
    skuprods = {}
    ChildSkus = []
    for sku,prod in SkuMap.items():
        prod = prod.split(',')
        if len(list(set(prod).intersection(prodcodes))) == len(prod):
            # print(ChildSkus)
            ChildSkus.append(sku)
            #TEST
            # for products in prod:
            #     skuprods[int(products)] = sku
            skuprods[sku] = prod


            #END TEST
            # print(prodcodes)
            # print(np.setdiff1d(prodcodes,prod))
            prodcodes = np.setdiff1d(prodcodes,prod)

            # if len(prodcodes) >0:
            #     print("**** Diff prod codes")
            #     print(prodcodes)
    vouc['skuprods'] = skuprods
    vouc['ASPSkus'] = ChildSkus
    return vouc


def customrorders(orderid):

    with conn.cursor() as cursor:
        sql = """select count(*) AS cnt
                    FROM uaudio.uad_custom
                    where orders_id = %s
                    """
        cursor.execute(sql, (orderid,))
        result = cursor.fetchone()
        if result['cnt'] > 0:
            return True
        return False


def customorderrecord(order, product_catalog, conn1):

    data = {}
    data['purchase_type'] = 'store'
    data['item_type'] = 'custom'
    data['order_id'] = order['entity_id']
    # data['item_id'] =
    data['customer_id'] = order['customer_id']
    data['order_sku'] = order['sku']
    data['voucher_serial'] = order['vouchers_serial']
    data['created_at'] = order['created_at']
    data['list_price'] = 1

    cur = conn1.cursor()
    insert_quey = """INSERT INTO public.royalty values (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s)"""
    cur.execute(insert_quey, (data['purchase_type'], data['item_type'], data['order_id'],
                              0, data['customer_id'], data['order_sku'],
                              data['voucher_serial'], '',
                              '', data['created_at'], data['list_price'], ''))
    conn1.commit()
    print(data)


def builddata(orderitem, product_catalog, conn1):
    '''
    Takes each order line and break down into the multiple entries (one for each ASP Sku)
    refers product catalog and matches the list price
    :param orderitem:
    :param product_catalog:
    :return:
    '''

    ## Used for deciding on the list price
    owned_productcodes = []
    owned_products = ownedproducts(orderitem)
    for prodcodes in owned_products:
        owned_productcodes.append(int(prodcodes[2:]))

    totallistprice = listpricesum(orderitem, product_catalog, owned_productcodes)
    # print(totallistprice)

    for skus, prod in orderitem['skuprods'].items():
        prod=[int(x) for x in prod]
        if len(list(set(prod).intersection(owned_productcodes))) == len(prod):
            continue
        else:

            data = {}
            data['purchase_type'] = 'store'
            data['order_id'] = orderitem['orderid']
            data['item_id'] = orderitem['item_id']
            data['customer_id'] = orderitem['customer_id']
            data['order_sku'] = orderitem['ordersku']
            data['voucher_serial'] = orderitem['voucherserial']
            # data['prodcode'] = prod
            # if prod in owned_productcodes:
            #     data['status'] = 'owned'
            # else:
            #     data['status'] = 'issued'
            data['sku'] = skus
            data['created_at']= '{:%Y-%m-%d %H:%M:%S}'.format(orderitem['created_at'])

            # List Price
            # For every SKU, if the customer already owns the products (in owner sku) from catalog products.
            # If yes, considers owner_discount as list price else choose price as list_price
            # ownersku = product_catalog.at[skus, 'owner_sku']
            # if ownersku:
            #     ownersku = list(ownersku.split(','))
            listprice = product_catalog.at[skus, 'price']

            # Owner discount on the list price
            # if ownersku:
            #     for prodcodes in ownersku:
            #         prodcode = int(prodcodes[2:])
            #         if prodcode in owned_productcodes:
            #             listprice = product_catalog.at[skus, 'owner_discount']


            data['list_price'] = '{0:.2f}'.format(listprice)
            data['pro_rata'] = round((listprice / totallistprice)*100, 2)

            ##Data Insertion
            cur = conn1.cursor()
            insert_quey = """INSERT INTO public.royalty values (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s)"""
            cur.execute(insert_quey, (data['purchase_type'], 'purchase', data['order_id'],
                                data['item_id'], data['customer_id'], data['order_sku'], data['voucher_serial'],'',
                                data['sku'], data['created_at'], data['list_price'], data['pro_rata']))
            conn1.commit()
            print(data)



def buildcustomdata(order, product_catalog, conn1):
    """

    :param order:
    :return:
    """
    with conn.cursor() as cursor:
        sql = """   SELECT c.*
                    FROM uaudio.uad_custom c LEFT JOIN uaudio.vouchers v
                    ON (c.vouchers_serial = v.vouchers_serial AND v.voucher_type = 'purchase')
                    WHERE c.orders_id = %s
                    """
        cursor.execute(sql, (order['entity_id'],))
        custom_rec = cursor.fetchall()
    # REPLACE(sp.skus_id, IF(sp.skus_id LIKE 'UAD-2%', 'UAD-2','UAD-1'), 'UAD') AS sku_id,

    for custrec in custom_rec:
        with conn1.cursor() as cursor1:

            sql = """ SELECT purchase_type,item_type, order_id, item_id, customer_id,
                        order_sku, voucher_serial, custom_serial, sku, created_at, (list_price)* -1 , pro_rata
                        FROM public.royalty
                        WHERE order_id = %s AND item_type = 'custom'
                    """
            cursor1.execute(sql, (custrec['orders_id'],))
            record = list(cursor1.fetchone())
            # Update when the table structure changes
            record[9] = custrec['redeem_date']


            sql=""" INSERT INTO public.royalty values %s;"""
            cursor1.execute(sql, (tuple(record),))

        ### END TEST

        with conn.cursor() as cursor:
            sql = """select cr.*
                    from uaudio.uad_custom_redeem cr
                    where custom_id = %s
                    ORDER BY date DESC
                    LIMIT %s       
             """
            cursor.execute(sql, (custrec['id'], custrec['number_plugins'],))
            records = cursor.fetchall()
            # print(records)

            totallisprice = 0
            for orderitem in records:
                sku = orderitem['sku'].replace('UAD-2', 'UAD')
                listprice = product_catalog.at[sku, 'price']
                totallisprice += listprice

            # buildcustomdata(custrec, product_catalog)
            print(records)

            for orderitem in records:
                data = {}
                data['purchase_type'] = 'store'
                data['item_type'] = 'custom-redeem'
                data['order_id'] = order['entity_id']
                data['item_id'] = order['item_id']
                data['custom_id'] = orderitem['id']
                data['customer_id'] = order['customer_id']
                data['order_sku'] = order['sku']
                data['voucher_serial'] = order['vouchers_serial']
                data['custom_serial'] = orderitem['vouchers_serial']
                # data['prodcode'] = prod
                # if prod in owned_productcodes:
                #     data['status'] = 'owned'
                # else:
                #     data['status'] = 'issued'
                data['sku'] = orderitem['sku'].replace('UAD-2', 'UAD')
                data['created_at'] = '{:%Y-%m-%d %H:%M:%S}'.format(custrec['redeem_date'])
                data['list_price'] = product_catalog.at[data['sku'], 'price']
                data['pro_rata'] = round((data['list_price']/totallisprice)*100,2)

                cur = conn1.cursor()
                insert_quey = """INSERT INTO public.royalty values (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s)"""
                cur.execute(insert_quey, (data['purchase_type'], data['item_type'], data['order_id'],
                                          data['item_id'], data['customer_id'], data['order_sku'],
                                          data['voucher_serial'], data['custom_serial'],
                                          data['sku'], data['created_at'], data['list_price'], data['pro_rata']))
                conn1.commit()

                print(data)



def ownedproducts(orderitem):
    '''
    Queries the database to get the list of prodcodes owned by the customer which is not part of
    this order (mached by voucher serial) and owned before the order created date
    :param orderitem:
    :return: List of product codes
    '''
    with conn.cursor() as cursor:
        sql = """select products_code
                 from uaudio.customers_products_sw
                 where customers_id = %s 
                 AND vouchers_serial != %s
                 AND customers_products_sw_created < %s
                 """
        cursor.execute(sql, (orderitem['customer_id'], orderitem['voucherserial'], orderitem['created_at'], ))
        owned_products = cursor.fetchall()
    owned_productcodes = []
    for prodcodes in owned_products:
        owned_productcodes.append(prodcodes['products_code'])

    ## Testing .... Match royalty SKUS for the products owned
    # vouc ={'prodcodes': owned_productcodes}
    # ownedsks = getskusforprodcodes(vouc,buildskumap())
    # owned_productskus = ownedsks['ASPSkus']
    # owned_product = {'owned_productcodes': owned_productcodes, 'owned_productskus': owned_productskus }
    return owned_productcodes


def catalogproducts():
    '''
    Dataframe of product catalog is built
    :return: product catalog
    '''
    with conn.cursor() as cursor:
        sql = """ select 
                   sku
                  ,owner_discount
                  ,owner_sku
                  ,price
                  ,special_from_date
                  ,special_owner_discount
                  ,special_price
                  ,special_to_date
                from uaudio.catalog_product_flat_1
        """
        cursor.execute(sql)
        result = cursor.fetchall()
    df = pd.DataFrame(data=result)
    product_catalog = df.set_index('sku')
    print('\n Product Catalog :')
    print(product_catalog)
    return product_catalog



def listpricesum(orderitem, product_catalog, owned_productcodes):

    # totallistprice = 0
    # for skus in orderitem['skuprods'].keys():
    #     listprice = product_catalog.at[skus, 'price']
    #     totallistprice += listprice

    totallistprice = 0
    for skus, prod in orderitem['skuprods'].items():
        prod = [int(x) for x in prod]
        if len(list(set(prod).intersection(owned_productcodes))) == len(prod):
            continue
        else:
    #         ownersku = product_catalog.at[skus, 'owner_sku']
    #         if ownersku:
    #             ownersku = list(ownersku.split(','))
            listprice = product_catalog.at[skus, 'price']
    #
    #         if ownersku:
    #             for prodcodes in ownersku:
    #                 prodcode = int(prodcodes[2:])
    #                 if prodcode in owned_productcodes:
    #                     listprice = product_catalog.at[skus, 'owner_discount']
        totallistprice += listprice
    return totallistprice


if __name__ == '__main__':
    conn, conn1 = dbconnection()
    vouchers = getorders()
    processorders(vouchers, conn1)
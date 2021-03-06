import pymysql, numpy as np, pandas as pd, boto3, psycopg2, json, re, datetime, psycopg2.extras


def dbconnection():
    '''Database connection block
    Access the S3 buckets to get the credentials to access RDS
    Establish a connection to the AWS RDS Mysql
    :return: connection
    '''

   s3= boto3.client('s3')
	obj = s3.get_object(Bucket = 'secure.uaudio', Key='redshift.env.sh')
	content = (obj['Body'].read())
	env_var = content.decode('ascii')
	env_var = (re.split('\n', env_var))
	cred = {}

	for i in env_var:
	    i = (re.split('=', i))
	    if len(i) > 1: cred[i[0][7:]] = i[1]
	print(cred)

	live_conn = pymysql.connect(
	    host='db-rr1.uaudio.com',
	    user=cred['RDS_USER'],
	    password=cred['RDS_PASSWORD'],
	    cursorclass=pymysql.cursors.DictCursor
	)

    return live_conn


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
                 -- Testing
                 join uaudio.catalog_product_flat_1 c ON REPLACE(sp.skus_id, IF(sp.skus_id LIKE 'UAD-2%', 'UAD-2','UAD-1'), 'UAD')= c.sku
                 --
                 where s.skus_class_id IN ('UAD-2-PAID','UAD-1-PAID')
                 AND p.products_class_id IN ('UAD-2-SW','UAD-1-SW')                 
                 AND s.skus_status='Active'
                 AND s.skus_product_type='plugin'
                 AND s.skus_bundle=0
                 AND c.visibility != 1
                 GROUP BY sp.skus_id"""
        cursor.execute(sql)
        sku = cursor.fetchall()
        skumap = dict()
        for i in range(len(sku)):
            skumap[sku[i]['sku_id']] = sku[i]['product_codes']

        # Sorting logic
        revskumap = {v: k for k, v in skumap.items()}
        # skumap = (sorted(skumap.values(), key=lambda kv: kv[1]))
        skumap = (sorted(skumap.values(), key=lambda kv: -len(kv.split(','))))
        newskumap = dict()
        for ar in skumap:
            newskumap[revskumap[ar]] = ar
        print(newskumap)
        return newskumap


def processcredits():
    with conn.cursor() as cursor:
        sql = """
                        select i.order_id, i.item_id, i.updated_at,
                        o.state, o.status from uaudio.sales_flat_creditmemo c join uaudio.sales_flat_order o ON c.order_id = o.entity_id
                        join uaudio.sales_flat_order_item i on o.entity_id = i.order_id
                        where i.qty_refunded = 1 and i.updated_at between '2018-01-01' AND '2019-01-01'
                        UNION 
                        select
                          i.order_id,
                          i.item_id,
                          i.updated_at,
                          o.state,
                          o.status
                        from uaudio.sales_flat_order_item i JOIN uaudio.sales_flat_order o ON i.order_id = o.entity_id
                        where i.qty_refunded = 1 and i.updated_at between '2018-01-01' AND '2019-01-01'

                         """
        cursor.execute(sql)
        orders = cursor.fetchall()
        print(orders)

    if len(orders) > 0:
        for order in orders:
            print(order)
            if order['status'] == 'ua_chargeback':
                record_type = 'Chargeback'
            else:
                record_type = 'Credit'

            with conn1.cursor() as cursor:
                sql = """insert into dev.royalty (select purchase_type, item_type, order_id,order_increment_id, item_id,
                         customer_id, order_sku, voucher_serial, custom_serial, sku,%s, list_price, discounted_list_price, pro_rata, 
                         net_price * -1, base_net_price * -1, price_incl_tax * -1,
                         base_price_incl_tax * -1, tax_amount * -1, base_tax_amount * -1, discount_amount * -1, 
                         base_discount_amount * -1, 
                         special_price * -1, base_special_price * -1, owner_discount_amount * -1, 
                         base_owner_discount_amount * -1, special_owner_discount_price * -1 , base_special_owner_discount_price * -1,
                         voucher_amount * -1, base_voucher_amount * -1, order_currency_code, custom_history_id,
                         custom_id, hw_serialnumber, invoice_date, %s, %s, %s from dev.royalty r
                        where r.order_id = %s AND r.item_id = %s )"""
                cursor.execute(sql, (order['updated_at'], order['state'], order['status'], record_type,
                                     order['order_id'], order['item_id']))
                conn1.commit()


def customswap():
    with conn1.cursor() as cursor:
        sql = """select distinct custom_history_id from dev.royalty where custom_history_id is not null ;"""
        cursor.execute(sql)
        history_id = cursor.fetchall()
        history_ids = tuple([x[0] if x[0] is not None else 0 for x in history_id])

    with conn.cursor() as cursor:
        cond = " AND ch.id NOT IN " + str(history_ids) if len(history_id) != 0 else ''
        sql = """select distinct ch.id, c.id as custom_id, ch.custom_history_date, number_plugins from uaudio.uad_custom_history ch
                   join uaudio.uad_custom c ON c.id = ch.custom_id
                   where ch.custom_history_date > '2018-06-01'
                   """ + cond
        cursor.execute(sql)
        result = cursor.fetchall()

        for customorder in result:
            with conn1.cursor() as cursor:
                sql = """ UPDATE dev.royalty SET custom_history_id = %s WHERE custom_id = %s 
                            AND item_type = 'custom-redeem' AND custom_history_id is null
                        """
                #
                sql1 = """INSERT INTO dev.royalty select purchase_type, item_type, order_id, order_increment_id, item_id, customer_id,
                          order_sku, voucher_serial, custom_serial, sku, %s, list_price, discounted_list_price , pro_rata, net_price * -1, 
                          base_net_price * -1, price_incl_tax * -1, base_price_incl_tax * -1, tax_amount * -1, base_tax_amount * -1,
                          discount_amount * -1, base_discount_amount * -1, order_currency_code, custom_history_id, custom_id
                          from dev.royalty WHERE custom_id = %s AND item_type = 'custom-redeem' AND custom_history_id = %s """

                cursor.execute(sql, (customorder['id'], customorder['custom_id']))
                cursor.execute(sql1, (customorder['custom_history_date'], customorder['custom_id'], customorder['id'],))
                affected_rows = cursor.rowcount
                conn1.commit()

                if affected_rows > 0:
                    with conn1.cursor() as cursor:
                        sql2 = """select order_id AS entity_id, item_id, custom_id, customer_id, order_sku as sku, voucher_serial 
                                from dev.royalty WHERE custom_id = %s 
                                 AND list_price > 0"""
                        cursor.execute(sql2, (customorder['custom_id'],))
                        order = cursor.fetchone()
                        orders = {'entity_id': order[0], 'item_id': order[1], 'custom_id': order[2],
                                  'customer_id': order[3], 'sku': order[4], 'vouchers_serial': order[5]}
                        custrec = {}
                        custrec['id'] = customorder['custom_id']
                        custrec['number_plugins'] = customorder['number_plugins']
                        dollars = getinvoiceitemdetails(order[0], order[1])
                        purchase_type = 'store'
                        item_type = 'custom-redeem'

                        insertcustomdata(orders, dollars, custrec, purchase_type, item_type)


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
        sql = """select distinct v.vouchers_serial, v.voucher_type, o.entity_id, i.sku, i.item_id, o.customer_id, 
                                v.vouchers_created as created_at, i.additional_data, o.increment_id, o.state, o.status, 
                                o.customers_products_hw_groups_id
                    from uaudio.vouchers v 
                    JOIN uaudio.sales_flat_order o 
                    ON v.vouchers_purchase_ordernum = o.entity_id
                    JOIN uaudio.sales_flat_order_item i
                    ON i.vouchers_serial = v.vouchers_serial
                    where v.voucher_type = 'purchase' 
                    # AND vouchers_purchase_date = current_date 
                    # AND v.vouchers_serial = 'K4DC-XHF9-8DEN-TBH0'
                    AND o.entity_id  = 1064050
                    AND o.state IN ('complete', 'closed')
                    """
        cursor.execute(sql)
        result = cursor.fetchall()
        return result


def processorders(vouchers, conn1, SkuMap, product_catalog):
    for order in vouchers:
        print(order)
        dollars = getinvoiceitemdetails(order['entity_id'], order['item_id'])

        if order['voucher_type'] == 'purchase':
            purchase_type = 'store'
            iscustom = customrorders(order['vouchers_serial'])
            if iscustom and order['sku'][:10] == 'UAD-CUSTOM':
                issue_type = 'custom'
                customorderrecord(order, dollars, purchase_type, issue_type)
                buildcustomdata(order, product_catalog, dollars, conn1)

            else:
                vouc = getproductcodes(order['vouchers_serial'], order['voucher_type'], order['entity_id'],
                                       order['sku'], order['item_id'], order['customer_id'], order['created_at'],
                                       order['additional_data'], order['increment_id'], order['state'], order['status'],
                                       order['customers_products_hw_groups_id'])
                orderitem = getskusforprodcodes(vouc, SkuMap)
                issue_type = 'purchase_'
                builddata(orderitem, product_catalog, dollars, conn1, purchase_type, issue_type)
        else:
            pass
            # print("Not Store")
            # print(order)


def getproductcodes(vouchers, voucher_type, orderid, ordersku, itemid, customerid, createdat, additional_data,
                    increment_id, state, status, hw_group_id):
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
    if voucher_type == 'registration' or voucher_type == 'NAMMB2B':
        sql = """SELECT vp.products_code
                            FROM uaudio.vouchers_products vp 
                            WHERE vp.vouchers_serial = %s"""
    else:
        sql = """SELECT vp.products_code
                    FROM uaudio.vouchers_products vp 
                JOIN uaudio.customers_products_sw cps 
                ON(vp.vouchers_serial = cps.vouchers_serial 
                AND vp.products_code = cps.products_code) 
                WHERE vp.vouchers_serial = %s"""

    with conn.cursor() as cursor:
        cursor.execute(sql, (vouchers,))
        result = cursor.fetchall()
        products = []
        vouc = dict()
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
        vouc['state'] = state
        vouc['status'] = status
        vouc['hw_group_id'] = hw_group_id
        if additional_data is not None:
            vouc['discount_type'] = (json.loads(additional_data)).get("discount_type", 'None')
        else:
            vouc['discount_type'] = 'None'
        vouc['order_increment_id'] = increment_id

        # vouc['discount_type'] = ((json.loads(additional_data)).get("discount_type", 'None')) if not None else 'None'
    return vouc


def getskusforprodcodes(vouc, SkuMap):
    '''
    Using the list of product codes associated to a voucher, ASP Skus are built
    :param vouc:
    :param SkuMap:
    :return:
    '''
    # vouc has {'orderid': , 'voucherserial': , 'prodcodes': }
    skus = []
    for prodcode in vouc['prodcodes']:
        prodcode = str(prodcode[-5:])
        for key, value in SkuMap.items():
            if prodcode == value:
                skus.append(key)
    vouc['SKUs'] = skus
    prodcodes = []
    for prodcode in vouc['prodcodes']:
        prodcodes.append(prodcode[-5:])

    skuprods = dict()
    ChildSkus = []
    for sku, prod in SkuMap.items():
        prod = prod.split(',')
        if len(list(set(prod).intersection(prodcodes))) == len(prod):
            ChildSkus.append(sku)
            # TEST
            # for products in prod:
            #     skuprods[int(products)] = sku
            skuprods[sku] = prod

            # END TEST
            prodcodes = np.setdiff1d(prodcodes, prod)
    if len(prodcodes) > 0:
        for prod in prodcodes:
            for sku, products in SkuMap.items():
                products = products.split(',')
                for prodc in products:
                    if prodc == prod:
                        skuprods[sku] = prodc

    vouc['remaining_prodcodes'] = prodcodes
    vouc['skuprods'] = skuprods
    vouc['ASPSkus'] = ChildSkus
    return vouc


def customrorders(vouchers_serial):
    with conn.cursor() as cursor:
        sql = """select count(*) AS cnt
                    FROM uaudio.uad_custom
                    where vouchers_serial = %s
                    """
        cursor.execute(sql, (vouchers_serial,))
        result = cursor.fetchone()
        if result['cnt'] > 0:
            return True
        return False


def customorderrecord(order, dollars, purchase_type, issue_type):
    data = dict()
    data['purchase_type'] = purchase_type
    data['item_type'] = issue_type
    data['order_id'] = order['entity_id']
    data['order_increment_id'] = order['increment_id']
    data['customer_id'] = order['customer_id']
    data['order_sku'] = order['sku']
    data['voucher_serial'] = order['vouchers_serial']
    data['state'] = order['state']
    data['status'] = order['status']
    data['created_at'] = order['created_at']
    data['price_incl_tax'] = float(0 if dollars[0]['price_incl_tax'] is None else dollars[0]['price_incl_tax'])
    data['base_price_incl_tax'] = float(
        0 if dollars[0]['base_price_incl_tax'] is None else dollars[0]['base_price_incl_tax'])
    data['tax_amount'] = float(0 if dollars[0]['tax_amount'] is None else dollars[0]['tax_amount'])
    data['base_tax_amount'] = float(0 if dollars[0]['base_tax_amount'] is None else dollars[0]['base_tax_amount'])
    data['discount_amount'] = float(0 if dollars[0]['discount_amount'] is None else dollars[0]['discount_amount'])
    data['base_discount_amount'] = float(
        0 if dollars[0]['base_discount_amount'] is None else dollars[0]['base_discount_amount'])
    data['order_currency_code'] = dollars[0]['order_currency_code']
    data['net_price'] = data['price_incl_tax'] - data['tax_amount'] - data['discount_amount']
    data['base_net_price'] = data['base_price_incl_tax'] - data['base_tax_amount'] - data['base_discount_amount']
    data['voucher_amount'] = float(
        0 if dollars[0]['voucher_amount'] is None else dollars[0]['voucher_amount'])
    data['base_voucher_amount'] = float(
        0 if dollars[0]['base_voucher_amount'] is None else dollars[0]['base_voucher_amount'])

    data['custom_serial'] = data['sku'] = data['list_price'] = data['pro_rata'] = data['special_price'] = \
        data['base_special_price'] = data['owner_discount_amount'] = data['base_owner_discount_amount'] = \
        data['special_owner_discount_price'] = data['base_special_owner_discount_price'] = \
        data['custom_history_id'] = data['custom_id'] = data['item_id'] = data['discounted_list_price'] = None
    data['hw_serialnumber'] = dollars[0]['hw_serialnum'] if dollars[0]['hw_serialnum'] is not None else None
    data['invoice_date'] = dollars[0]['invoice_date'] if dollars[0]['invoice_date'] is not None else None

    insertdata(data)


def builddata(orderitem, product_catalog, dollars, conn1, purchase_type, issue_type):
    '''
    Takes each order line and break down into the multiple entries (one for each ASP Sku)
    refers product catalog and matches the list price
    :param orderitem:
    :param product_catalog:
    :return:
    '''

    owned_productcodes = []
    owned_products = ownedproducts(orderitem)
    for prodcodes in owned_products:
        owned_productcodes.append(int(prodcodes[2:]))

    totallistprice = listpricesum(orderitem, product_catalog, owned_productcodes, dollars)
    item_type = ''

    if issue_type.find('hw/sw') == -1:
        if isUltimate(orderitem['ordersku']):
            item_type = 'ultimate'
            if orderitem['discount_type'] == 'bundle_discount':
                item_type = 'ultimate-upgrade'
            elif orderitem['discount_type'] == 'complete_your_ultimate':
                item_type = 'complete_ur_ultimate'
        elif isBundle(orderitem['ordersku']) == 1:
            item_type = 'bundle'
            if orderitem['discount_type'] == 'bundle_discount':
                item_type = 'bundle_upgrade'
        elif isDiscounted(orderitem['ordersku'], owned_productcodes) == 1:
            item_type = 'standalone-upgrade'
        else:
            item_type = 'standalone'



    for skus, prod in orderitem['skuprods'].items():
        prod = [int(x) for x in prod]
        # Testing
        if orderitem['discount_type'] == 'bundle_discount' or orderitem['discount_type'] == 'complete_your_ultimate':
            # if item_type == 'ultimate-upgrade':
            if len(list(set(prod).intersection(owned_productcodes))) == len(prod):
                continue
            else:
                try:
                    listprice = product_catalog.at[(skus, dollars[0]['order_currency_code']), 'price']
                except KeyError:
                    listprice = 0

                if listprice != 0:
                    data = dict()
                    data['purchase_type'] = purchase_type
                    data['item_type'] = issue_type + item_type
                    data['order_id'] = orderitem['orderid']
                    data['order_increment_id'] = orderitem['order_increment_id']
                    data['item_id'] = orderitem['item_id']
                    data['customer_id'] = orderitem['customer_id']
                    data['order_sku'] = orderitem['ordersku']
                    data['voucher_serial'] = orderitem['voucherserial']
                    data['state'] = orderitem['state']
                    data['status'] = orderitem['status']
                    # data['prodcode'] = prod
                    # if prod in owned_productcodes:
                    #     data['status'] = 'owned'
                    # else:
                    #     data['status'] = 'issued'
                    data['sku'] = skus
                    data['created_at'] = '{:%Y-%m-%d %H:%M:%S}'.format(orderitem['created_at'])

                    # List Price
                    # For every SKU, if the customer already owns the products (in owner sku) from catalog products.
                    # If yes, considers owner_discount as list price else choose price as list_price

                    data['discounted_list_price'] = discount_price.at[(skus, dollars[0]['order_currency_code']), 'price'] if isDiscounted(skus, owned_productcodes) == 1 else None


                    '''ownersku = product_catalog.at[skus, 'owner_sku']
                    if ownersku:
                        ownersku = list(ownersku.split(','))

                    # Owner discount on the list price
                    data['owner_discount_price'] = None
                    if ownersku:
                        for prodcodes in ownersku:
                            prodcode = int(prodcodes[2:])
                            if prodcode in owned_productcodes:
                                data['owner_discount_price'] = product_catalog.at[skus, 'owner_discount']'''

                    data['list_price'] = '{0:.2f}'.format(listprice)

                    data['pro_rata'] = '{0:.3f}'.format((listprice / totallistprice) * 100)
                    prorata = float(listprice / totallistprice)
                    data['price_incl_tax'] = float(
                        0 if dollars[0]['price_incl_tax'] is None else dollars[0]['price_incl_tax']) * prorata
                    data['base_price_incl_tax'] = float(
                        0 if dollars[0]['base_price_incl_tax'] is None else dollars[0]['base_price_incl_tax']) * prorata
                    data['tax_amount'] = float(
                        0 if dollars[0]['tax_amount'] is None else dollars[0]['tax_amount']) * prorata
                    data['base_tax_amount'] = float(
                        0 if dollars[0]['base_tax_amount'] is None else dollars[0]['base_tax_amount']) * prorata
                    data['discount_amount'] = float(
                        0 if dollars[0]['discount_amount'] is None else dollars[0]['discount_amount']) * prorata
                    data['base_discount_amount'] = float(
                        0 if dollars[0]['base_discount_amount'] is None else dollars[0][
                            'base_discount_amount']) * prorata
                    ## Testing
                    data['special_price'] = float(
                        0 if dollars[0]['special_price'] is None else dollars[0]['special_price']) * prorata
                    data['base_special_price'] = float(0 if dollars[0]['base_special_price'] is None
                                                       else dollars[0]['base_special_price']) * prorata

                    data['owner_discount_amount'] = float(
                        0 if dollars[0]['owner_discount_price'] is None else dollars[0][
                            'owner_discount_price']) * prorata
                    data['base_owner_discount_amount'] = float(0 if dollars[0]['base_owner_discount_price'] is None
                                                               else dollars[0]['base_owner_discount_price']) * prorata

                    data['special_owner_discount_price'] = float(
                        0 if dollars[0]['special_owner_discount_price'] is None else dollars[0][
                            'special_owner_discount_price']) * prorata
                    data['base_special_owner_discount_price'] = float(
                        0 if dollars[0]['base_special_owner_discount_price'] is None
                        else dollars[0]['base_special_owner_discount_price']) * prorata
                    ###

                    data['order_currency_code'] = dollars[0]['order_currency_code']
                    data['net_price'] = data['price_incl_tax'] - data['tax_amount'] - data['discount_amount']
                    data['base_net_price'] = data['base_price_incl_tax'] - data['base_tax_amount'] - data[
                        'base_discount_amount']
                    data['voucher_amount'] = float(
                        0 if dollars[0]['voucher_amount'] is None else dollars[0]['voucher_amount']) * prorata
                    data['base_voucher_amount'] = float(
                        0 if dollars[0]['base_voucher_amount'] is None else dollars[0]['base_voucher_amount']) * prorata

                    data['custom_serial'] = data['custom_history_id'] = data['custom_id'] = None
                    data['hw_serialnumber'] = dollars[0]['hw_serialnum'] if dollars[0][
                                                                                'hw_serialnum'] is not None else None
                    data['invoice_date'] = dollars[0]['invoice_date'] if dollars[0][
                                                                             'invoice_date'] is not None else None

                    insertdata(data)
        else:
            try:
                listprice = product_catalog.at[(skus, dollars[0]['order_currency_code']), 'price']
            except KeyError:
                listprice = 0

            if listprice != 0:
                data = dict()
                data['purchase_type'] = purchase_type
                data['item_type'] = issue_type + item_type
                data['order_id'] = orderitem['orderid']
                data['order_increment_id'] = orderitem['order_increment_id']
                data['item_id'] = orderitem['item_id']
                data['customer_id'] = orderitem['customer_id']
                data['order_sku'] = orderitem['ordersku']
                data['voucher_serial'] = orderitem['voucherserial']
                data['state'] = orderitem['state']
                data['status'] = orderitem['status']
                # data['prodcode'] = prod
                # if prod in owned_productcodes:
                #     data['status'] = 'owned'
                # else:
                #     data['status'] = 'issued'
                data['sku'] = skus
                data['created_at'] = '{:%Y-%m-%d %H:%M:%S}'.format(orderitem['created_at']) if orderitem[
                                                                                                   'created_at'] is not None else None

                # List Price
                # For every SKU, if the customer already owns the products (in owner sku) from catalog products.
                # If yes, considers owner_discount as list price else choose price as list_price
                # ownersku = product_catalog.at[skus, 'owner_sku']
                # if ownersku:
                #     ownersku = list(ownersku.split(','))

                # Owner discount on the list price
                # if ownersku:
                #     for prodcodes in ownersku:
                #         prodcode = int(prodcodes[2:])
                #         if prodcode in owned_productcodes:
                #             listprice = product_catalog.at[skus, 'owner_discount']
                try:
                    data['discounted_list_price'] = discount_price.at[(skus, dollars[0]['order_currency_code']), 'price'] if item_type == 'standalone-upgrade' else None
                except KeyError:
                    data['discounted_list_price'] = None
                # data['discounted_list_price'] = discount_price.at[(skus, dollars[0]['order_currency_code']), 'price'] if item_type == 'standalone-upgrade' else None
                data['list_price'] = '{0:.2f}'.format(listprice)
                data['pro_rata'] = '{0:.3f}'.format((listprice / totallistprice) * 100)
                prorata = float(listprice / totallistprice)
                data['price_incl_tax'] = float(
                    0 if dollars[0]['price_incl_tax'] is None else dollars[0]['price_incl_tax']) * prorata
                data['base_price_incl_tax'] = float(
                    0 if dollars[0]['base_price_incl_tax'] is None else dollars[0]['base_price_incl_tax']) * prorata
                data['tax_amount'] = float(
                    0 if dollars[0]['tax_amount'] is None else dollars[0]['tax_amount']) * prorata
                data['base_tax_amount'] = float(
                    0 if dollars[0]['base_tax_amount'] is None else dollars[0]['base_tax_amount']) * prorata
                data['discount_amount'] = float(
                    0 if dollars[0]['discount_amount'] is None else dollars[0]['discount_amount']) * prorata
                data['base_discount_amount'] = float(
                    0 if dollars[0]['base_discount_amount'] is None else dollars[0]['base_discount_amount']) * prorata
                data['order_currency_code'] = dollars[0]['order_currency_code']
                data['net_price'] = data['price_incl_tax'] - data['tax_amount'] - data['discount_amount']
                data['base_net_price'] = data['base_price_incl_tax'] - data['base_tax_amount'] - data[
                    'base_discount_amount']
                data['voucher_amount'] = float(
                    0 if dollars[0]['voucher_amount'] is None else dollars[0]['voucher_amount']) * prorata
                data['base_voucher_amount'] = float(
                    0 if dollars[0]['base_voucher_amount'] is None else dollars[0]['base_voucher_amount']) * prorata
                data['custom_serial'] = data['special_price'] = data['base_special_price'] = data[
                    'owner_discount_amount'] = \
                    data['base_owner_discount_amount'] = data['special_owner_discount_price'] = data[
                    'base_special_owner_discount_price'] = \
                    data['custom_history_id'] = data['custom_id'] = None
                data['hw_serialnumber'] = dollars[0]['hw_serialnum'] if dollars[0]['hw_serialnum'] is not None else None
                data['invoice_date'] = dollars[0]['invoice_date'] if dollars[0]['invoice_date'] is not None else None

                insertdata(data)


def insertdata(data):
    cur = conn1.cursor()
    insert_quey = """INSERT INTO dev.royalty(purchase_type, item_type, order_id, order_increment_id, item_id,
                                                customer_id, order_sku, voucher_serial, custom_serial, sku, created_at, list_price,
                                                discounted_list_price, pro_rata, net_price, base_net_price, price_incl_tax, base_price_incl_tax,
                                                tax_amount, base_tax_amount, discount_amount, base_discount_amount,
                                                special_price, base_special_price, owner_discount_amount,
                                                base_owner_discount_amount, special_owner_discount_price,
                                                base_special_owner_discount_price, voucher_amount, base_voucher_amount,
                                                order_currency_code, custom_history_id, custom_id, hw_serialnumber, invoice_date,
                                                order_state, order_status)
                                                values (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s,
                                                %s,%s, %s, %s, %s, %s ,%s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s)"""
    try:
        cur.execute(insert_quey,
                    (data['purchase_type'], data['item_type'], data['order_id'], data['order_increment_id'],
                     data['item_id'], data['customer_id'], data['order_sku'], data['voucher_serial'],
                     data['custom_serial'],
                     data['sku'], data['created_at'], data['list_price'], data['discounted_list_price'], data['pro_rata'], data['net_price'],
                     data['base_net_price'], data['price_incl_tax'], data['base_price_incl_tax'],
                     data['tax_amount'], data['base_tax_amount'], data['discount_amount'], data['base_discount_amount'],
                     data['special_price'], data['base_special_price'], data['owner_discount_amount'],
                     data['base_owner_discount_amount'], data['special_owner_discount_price'],
                     data['base_special_owner_discount_price'], data['voucher_amount'],
                     data['base_voucher_amount'], data['order_currency_code'], data['custom_history_id'],
                     data['custom_id'], data['hw_serialnumber'], data['invoice_date'], data['state'], data['status']))
        conn1.commit()
    except psycopg2.Error as e:
        print("Error : ", e)
        return None


def buildcustomdata(order, product_catalog, dollars, conn1):
    """

    :param order:
    :return:
    """
    issue_type = 'purchase'
    custom_rec = getcustomrecord(order['vouchers_serial'])
    # REPLACE(sp.skus_id, IF(sp.skus_id LIKE 'UAD-2%', 'UAD-2','UAD-1'), 'UAD') AS sku_id,

    for custrec in custom_rec:
        with conn1.cursor() as cursor1:
            sql = """ SELECT purchase_type,item_type, order_id, order_increment_id, item_id, customer_id,
                        order_sku, voucher_serial, custom_serial, sku, created_at, list_price , discounted_list_price, pro_rata,
                        net_price * -1, base_net_price *-1,
                        price_incl_tax * -1, base_price_incl_tax * -1, tax_amount * -1, base_tax_amount * -1,
                        discount_amount * -1, base_discount_amount * -1, special_price * -1, base_special_price * -1, 
                        owner_discount_amount * -1, base_owner_discount_amount * -1, special_owner_discount_price * -1, 
                        base_special_owner_discount_price * -1, voucher_amount *-1, base_voucher_amount *-1,
                        order_currency_code, custom_history_id, custom_id, hw_serialnumber, invoice_date,
                        order_state, order_status
                        FROM dev.royalty
                        WHERE voucher_serial = %s 
--                        AND item_type = 'custom'
                        AND item_type IN ('custom', 'hw/sw_custom', 'nammb2b_custom')
                    """
            cursor1.execute(sql, (order['vouchers_serial'],))
            record = list(cursor1.fetchone())
            # Update when the table structure changes
            record[10] = custrec['redeem_date']

            sql = """ INSERT INTO dev.royalty values %s;"""
            cursor1.execute(sql, (tuple(record),))

            purchase_type = record[0]
            item_type = record[1] + '_redeem'

            insertcustomdata(order, dollars, custrec, purchase_type, item_type)


def getcustomrecord(vouchers_serial):
    with conn.cursor() as cursor:
        sql = """   SELECT c.*
                    FROM uaudio.uad_custom c
                    WHERE c.vouchers_serial = %s
                    """
        cursor.execute(sql, (vouchers_serial,))
        custom_rec = cursor.fetchall()

        return custom_rec


def iscustomredeemed(customid):
    with conn.cursor() as cursor:
        sql = """   SELECT count(*) as cnt
                FROM uaudio.uad_custom_redeem c
                WHERE c.custom_id = %s
                """
        cursor.execute(sql, (customid,))
        cnt = cursor.fetchone()

        return True if cnt != 0 else False


def insertcustomdata(order, dollars, custrec, purchase_type, item_type):
    with conn.cursor() as cursor:
        sql = """select cr.*
                    from uaudio.uad_custom_redeem cr
                    where custom_id = %s AND qty = 1
                    ORDER BY date DESC
                    LIMIT %s      
             """
        cursor.execute(sql, (custrec['id'], custrec['number_plugins'],))
        records = cursor.fetchall()

        totallistprice = 0
        for orderitem in records:
            sku = orderitem['sku'].replace('UAD-2', 'UAD')
            listprice = product_catalog.at[(sku, dollars[0]['order_currency_code']), 'price']
            totallistprice += listprice

        # buildcustomdata(custrec, product_catalog)

        for orderitem in records:
            data = dict()
            data['purchase_type'] = purchase_type
            data['item_type'] = item_type
            data['order_id'] = order['entity_id']
            data['order_increment_id'] = order['increment_id']
            data['item_id'] = order['item_id']
            data['custom_id'] = orderitem['custom_id']
            data['customer_id'] = order['customer_id']
            data['order_sku'] = order['sku']
            data['voucher_serial'] = order['vouchers_serial']
            data['state'] = order['state']
            data['status'] = order['status']
            data['custom_serial'] = orderitem['vouchers_serial']
            data['custom_history_id'] = orderitem['history_id']
            # data['prodcode'] = prod
            # if prod in owned_productcodes:
            #     data['status'] = 'owned'
            # else:
            #     data['status'] = 'issued'
            data['discounted_list_price'] = None
            data['sku'] = orderitem['sku'].replace('UAD-2', 'UAD')
            data['created_at'] = '{:%Y-%m-%d %H:%M:%S}'.format(orderitem['date'])
            data['list_price'] =  product_catalog.at[(data['sku'], dollars[0]['order_currency_code']), 'price']
            data['pro_rata'] = '{0:.3f}'.format((data['list_price'] / totallistprice) * 100)
            prorata = float(data['list_price'] / totallistprice)
            data['price_incl_tax'] = float(
                0 if dollars[0]['price_incl_tax'] is None else dollars[0]['price_incl_tax']) * prorata
            data['base_price_incl_tax'] = float(
                0 if dollars[0]['base_price_incl_tax'] is None else dollars[0]['base_price_incl_tax']) * prorata
            data['tax_amount'] = float(0 if dollars[0]['tax_amount'] is None else dollars[0]['tax_amount']) * prorata
            data['base_tax_amount'] = float(
                0 if dollars[0]['base_tax_amount'] is None else dollars[0]['base_tax_amount']) * prorata
            data['discount_amount'] = float(
                0 if dollars[0]['discount_amount'] is None else dollars[0]['discount_amount']) * prorata
            data['base_discount_amount'] = float(
                0 if dollars[0]['base_discount_amount'] is None else dollars[0]['base_discount_amount']) * prorata
            data['order_currency_code'] = dollars[0]['order_currency_code']
            data['custom_history_id'] = orderitem['history_id']
            data['net_price'] = data['price_incl_tax'] - data['tax_amount'] - data['discount_amount']
            data['base_net_price'] = data['base_price_incl_tax'] - data['base_tax_amount'] - data[
                'base_discount_amount']
            data['voucher_amount'] = float(
                0 if dollars[0]['voucher_amount'] is None else dollars[0]['voucher_amount']) * prorata
            data['base_voucher_amount'] = float(
                0 if dollars[0]['base_voucher_amount'] is None else dollars[0]['base_voucher_amount']) * prorata

            data['special_price'] = data['base_special_price'] = data['owner_discount_amount'] = \
                data['base_owner_discount_amount'] = data['special_owner_discount_price'] = \
                data['base_special_owner_discount_price'] = None

            data['hw_serialnumber'] = dollars[0]['hw_serialnum'] if dollars[0]['hw_serialnum'] is not None else None
            data['invoice_date'] = dollars[0]['invoice_date'] if dollars[0]['invoice_date'] is not None else None

            insertdata(data)


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
                 AND customers_products_hw_groups_id = %s
                 """
        cursor.execute(sql, (
            orderitem['customer_id'], orderitem['voucherserial'], orderitem['created_at'], orderitem['hw_group_id']))
        owned_products = cursor.fetchall()
    owned_productcodes = []
    for prodcodes in owned_products:
        owned_productcodes.append(prodcodes['products_code'])

    return owned_productcodes


def catalogproducts():
    '''
    Dataframe of product catalog is built
    :return: product catalog
    '''
    with conn.cursor() as cursor:
        sql = """ select
                  sku,
                  price,
                  'USD' AS currency
                    from uaudio.catalog_product_flat_1
                    WHERE sku is not null
                  UNION
                  select
                  sku,
                  price,
                  currency
                      from uaudio.catalog_product_compound_price cp
                        join uaudio.catalog_product_entity e
                        ON cp.product_id = e.entity_id
                  UNION
                    select
                      'UAD-DISTORTION-ESSENTIALS' as sku,
                      249                         as price,
                      'USD'                       as currency
                    UNION
                    select
                      'UAD-DISTORTION-ESSENTIALS' as sku,
                      249                         as price,
                      'EUR'                       as currency
                    UNION
                    select
                      'UAD-DISTORTION-ESSENTIALS' as sku,
                      189                         as price,
                      'GBP'                       as currency
        """
        cursor.execute(sql)
        result = cursor.fetchall()
    df = pd.DataFrame(data=result)
    product_catalog = df.set_index(['sku', 'currency'])
    return product_catalog


def discountprice():
    with conn.cursor() as cursor:
        sql = """ select
                  e.sku,
                  cp.price,
                  f.owner_sku,
                  cp.currency
                    from uaudio.catalog_product_owner_discount cp
                  join uaudio.catalog_product_entity e ON cp.product_id = e.entity_id
                  left join uaudio.catalog_product_flat_1 f ON f.sku = e.sku
        """
        cursor.execute(sql)
        result = cursor.fetchall()
    df = pd.DataFrame(data=result)
    discount_price = df.set_index(['sku', 'currency'])
    return discount_price


def listpricesum(orderitem, product_catalog, owned_productcodes, dollars):
    # totallistprice = 0
    # for skus in orderitem['skuprods'].keys():
    #     listprice = product_catalog.at[skus, 'price']
    #     totallistprice += listprice
    totallistprice = 0
    for skus, prod in orderitem['skuprods'].items():
        prod = [int(x) for x in prod]
        if orderitem['discount_type'] == 'bundle_discount' or orderitem['discount_type'] == 'complete_your_ultimate':
            if len(list(set(prod).intersection(owned_productcodes))) == len(prod):
                continue
            else:
                try:
                    listprice = product_catalog.at[(skus, dollars[0]['order_currency_code']), 'price']
                except KeyError:
                    listprice = 0
        else:
            try:
                listprice = product_catalog.at[(skus, dollars[0]['order_currency_code']), 'price']
            except KeyError:
                listprice = 0
        #         ownersku = product_catalog.at[skus, 'owner_sku']
        #         if ownersku:
        #             ownersku = list(ownersku.split(','))

        #
        #         if ownersku:
        #             for prodcodes in ownersku:
        #                 prodcode = int(prodcodes[2:])
        #                 if prodcode in owned_productcodes:
        #                     listprice = product_catalog.at[skus, 'owner_discount']
        totallistprice += listprice
    return totallistprice


def getinvoiceitemdetails(order_id, item_id):
    with conn.cursor() as cursor:
        sql = """select it.price_incl_tax, it.base_price_incl_tax, it.tax_amount, it.base_tax_amount,
                    it.discount_amount, it. base_discount_amount, i.order_currency_code,
                    ot.special_price, ot.base_special_price, ot.owner_discount_price, ot.base_owner_discount_price,
                    ot.special_owner_discount_price, ot.base_special_owner_discount_price, 
                    ((it.price_incl_tax - coalesce(it.tax_amount, 0)) * coalesce(i.customer_balance_amount, 0)) / (
                      select sum(price_incl_tax) - coalesce(sum(b.tax_amount), 0) total from uaudio.sales_flat_invoice a
                      join uaudio.sales_flat_invoice_item b ON a.entity_id = b.parent_id where a.order_id = %s) voucher_amount,
                    ((it.base_price_incl_tax - coalesce(it.base_tax_amount, 0)) * coalesce(i.base_customer_balance_amount, 0)) / (
                      select sum(base_price_incl_tax) - coalesce(sum(b.base_tax_amount), 0) total from uaudio.sales_flat_invoice a
                      join uaudio.sales_flat_invoice_item b ON a.entity_id = b.parent_id where a.order_id = %s) base_voucher_amount,
                      NULL as hw_serialnum, NULL as invoice_date
                    from uaudio.sales_flat_order o
                    join uaudio.sales_flat_order_item ot on o.entity_id = ot.order_id
                    join uaudio.sales_flat_invoice i on o.entity_id = i.order_id
                    left outer join uaudio.sales_flat_invoice_item it on (i.entity_id = it.parent_id and ot.item_id = it.order_item_id)
                    where o.entity_id = %s AND it.order_item_id = %s        
        """
        cursor.execute(sql, (order_id, order_id, order_id, item_id,))
        dollarvalues = cursor.fetchall()
        return dollarvalues


def isUltimate(sku):
    with conn.cursor() as cursor:
        sql = """ select s.skus_product_type FROM (
                select REPLACE(skus_id,  IF(
                    skus_id LIKE 'UAD-2%%', 'UAD-2','UAD-1'), 'UAD') AS sku_id, skus_product_type
                from uaudio.skus) s
                where s.sku_id = %s
              """
        cursor.execute(sql, (sku,))
        res = cursor.fetchone()

        if res is None:
            return 0
        elif res['skus_product_type'] == 'upgrade':
            return 1
        else:
            return 0

        # return 1 if res['skus_product_type'] == 'upgrade' else 0


def isBundle(sku):
    with conn.cursor() as cursor:
        sql = """select s.skus_bundle FROM (
                select REPLACE(skus_id,  IF(
                    skus_id LIKE 'UAD-2%%', 'UAD-2','UAD-1'), 'UAD') AS sku_id, skus_bundle
                from uaudio.skus) s 
                where s.sku_id = %s"""
        cursor.execute(sql, (sku,))
        res = cursor.fetchone()

        if res is None:
            return 0
        else:
            return res['skus_bundle']
        # return res['skus_bundle']


def isDiscounted(sku, owned_productcodes):

    try:
        ownersku = discount_price.at[(sku, 'USD'), 'owner_sku']
        if ownersku:
            ownersku = list(ownersku.split(','))
        count = 0
        if ownersku:
            for prodcodes in ownersku:
                prodcode = int(prodcodes[2:])
                if prodcode in owned_productcodes:
                    count += 1

            if len(ownersku) == count:
                return 1
            else: return 0
    except KeyError:
        return 0


def getchannelorders(product_catalog, SkuMap):
    with conn.cursor() as cursor:
        sql = """select *, nb.id AS namm_id from uaudio.vouchers v 
                 # join uaudio.vouchers_products vp on v.vouchers_serial = vp.vouchers_serial
                 left outer join uaudio.nammb2b nb on v.vouchers_purchase_ordernum = nb.po_number
                 WHERE v.voucher_type = 'promo' AND
                 # WHERE v.voucher_type != 'purchase' AND
                 # v.vouchers_serial = '3XWX-VX88-8KE6-13E0' AND
                 v.vouchers_created BETWEEN '2018-02-01' AND '2018-12-31' 
                 order by v.vouchers_serial      
        """
        cursor.execute(sql)
        promoorders = cursor.fetchall()

        for order in promoorders:
            if order['voucher_type'] == 'promo':
                if order['skus_id'] == 'UAD-CUSTOM-N':
                    with conn.cursor() as cursor:
                        sql = """select cr.* from uaudio.vouchers v
                                 join uaudio.uad_custom c on v.vouchers_serial = c.vouchers_serial
                                 join uaudio.uad_custom_redeem cr on c.id = cr.custom_id
                                 where v.vouchers_serial = %s AND qty = 1
                                 ORDER BY cr.date DESC 
                                """
                        cursor.execute(sql, (order['vouchers_serial'],))
                        customorder = cursor.fetchall()

                        for cust in customorder:
                            data = dict()
                            data['purchase_type'] = 'channel'
                            data['item_type'] = 'promo-custom'
                            data['custom_id'] = cust['custom_id']
                            data['customer_id'] = order['customers_id']
                            data['order_sku'] = order['vouchers_admin_reason'].strip()
                            data['voucher_serial'] = order['vouchers_serial']
                            data['sku'] = cust['sku'].replace('UAD-2', 'UAD')
                            data['created_at'] = '{:%Y-%m-%d %H:%M:%S}'.format(cust['date'])
                            data['list_price'] = product_catalog.at[(data['sku'], 'USD'), 'price']

                            cur = conn1.cursor()
                            insert_quey = """INSERT INTO dev.royalty(purchase_type, item_type, customer_id, order_sku, 
                                                                voucher_serial, custom_serial, sku, created_at, list_price) 
                                                    values (%s, %s, %s,%s, %s, %s,%s, %s, %s)"""

                            cur.execute(insert_quey, (data['purchase_type'], data['item_type'],
                                                      data['customer_id'], data['order_sku'],
                                                      data['voucher_serial'], cust['vouchers_serial'],
                                                      data['sku'], data['created_at'], data['list_price'],))
                            conn1.commit()

                else:
                    data = dict()
                    data['purchase_type'] = 'channel'
                    data['item_type'] = 'promo'
                    data['customer_id'] = order['customers_id']
                    data['order_sku'] = order['vouchers_admin_reason'].strip()
                    data['voucher_serial'] = order['vouchers_serial']
                    data['sku'] = order['skus_id'].replace('UAD-2', 'UAD')
                    data['created_at'] = '{:%Y-%m-%d %H:%M:%S}'.format(order['vouchers_created'])
                    listprice = product_catalog.at[(data['sku'], 'USD'), 'price']
                    data['list_price'] = '{0:.2f}'.format(listprice)

                    cur = conn1.cursor()
                    insert_quey = """INSERT INTO dev.royalty(purchase_type, item_type, customer_id, order_sku, 
                                                                                voucher_serial, sku, created_at, list_price) 
                                                                    values (%s, %s, %s,%s, %s, %s, %s, %s)"""
                    cur.execute(insert_quey, (data['purchase_type'], data['item_type'],
                                              data['customer_id'], data['order_sku'],
                                              data['voucher_serial'], data['sku'], data['created_at'],
                                              data['list_price'],))
                    conn1.commit()
            elif order['voucher_type'] == 'registration' or order['voucher_type'] == 'nammb2b':
                voucher = getproductcodes(order['vouchers_serial'], order['voucher_type'],
                                          order['vouchers_purchase_ordernum'],
                                          order['skus_id'], None, order['customers_id'], order['vouchers_created'],
                                          None, None, None, None, None)

                orderitem = getskusforprodcodes(voucher, SkuMap)

                if order['voucher_type'] == 'registration':
                    with conn.cursor() as cursor:
                        sql = """select customers_products_hw_serial from uaudio.customers_products_hw
                        where vouchers_serial = %s
                        """
                        cursor.execute(sql, (order['vouchers_serial'],))
                        serialhist = cursor.fetchone()

                    dollars = list()
                    dollar = dict()
                    dollar['discount_amount'] = dollar['base_discount_amount'] = dollar['voucher_amount'] = \
                        dollar['base_voucher_amount'] = dollar['price_incl_tax'] = dollar['base_price_incl_tax'] = \
                        dollar['hw_serialnum'] = dollar['tax_amount'] = dollar['base_tax_amount'] = orderitem['orderid'] \
                        = orderitem['item_id'] = order['entity_id'] = order['item_id'] = dollar['invoice_date'] = None
                    dollar['order_currency_code'] = 'USD'

                    if serialhist is not None:

                        with conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                            sql = " select ordernum, orderline, trandate from rpt.serialnumberhistory where serialnumber = %s AND shipped =1 "
                            cursor.execute(sql, (serialhist['customers_products_hw_serial'],))
                            serialdetail = cursor.fetchone()

                        if serialdetail:
                            orderitem['orderid'] = order['entity_id'] = serialdetail['ordernum']
                            orderitem['item_id'] = order['item_id'] = serialdetail['orderline']

                        with conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                            sql = """ select sm.custid, sm.groupcode, sm.partnum, usdprice, localprice, coalesce(nullif(usdtaxamt,0)/quantity,0) as usdtaxamt, 
                                        coalesce(nullif(localtaxamt,0)/quantity,0) as localtaxamt, sm.currencycode, sm.ordernum,sm.orderline, ih.invoicedate
                                        from rpt.salesmargindetail sm join rpt.serialnumberhistory sn
                                        ON sn.ordernum = sm.ordernum AND sn.orderline = sm.orderline
                                        LEFT JOIN erp.invchead ih ON sm.ordernum = ih.ordernum
                                      where sn.serialnumber = %s
                                      AND sm.shipstatus IN ('Shipped','Invoiced')  
                                      AND sn.shipped = 1
                                    """
                            cursor.execute(sql, (serialhist['customers_products_hw_serial'],))
                            customorder = cursor.fetchone()

                            dollar['hw_serialnum'] = serialhist['customers_products_hw_serial']

                            if customorder:
                                dollar['invoice_date'] = customorder['invoicedate'] \
                                    if customorder['invoicedate'] is not None else None

                                p = re.compile(r'\bCUSTOM*\b |\bULTIMATE[0-9]*\b', flags=re.I | re.X)
                                m = p.search(orderitem['ordersku'])

                                if m:
                                    with conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                                        sql = """select  pp.baseprice from erp.custgruppricelst cp Join erp.pricelst p ON cp.listcode = p.listcode
                                                   join erp.pricelstparts pp ON pp.listcode = p.listcode
                                                  where groupcode = %s
                                                  AND p.currencycode = %s
                                                  AND %s between startdate and enddate
                                                  AND partnum = (
                                                select ua.character01 as part_num from erp.customer c JOIN erp.customer_ud cu ON c.sysrowid = cu.foreignsysrowid
                                                  JOIN ice.ud100a ua ON cu.partlistid_c = ua.key1
                                                  join ice.ud01 ud ON ua.character02 = ud.shortchar02
                                                where c.custid = %s
                                                      AND ud.shortchar07 = %s
                                                limit 1)
                                        """
                                        cursor.execute(sql, (
                                            customorder['groupcode'], customorder['currencycode'],
                                            serialdetail['trandate'],
                                            customorder['custid'], orderitem['ordersku'],))
                                        base_price = cursor.fetchone()
                                        local_price_delta = round(customorder['localprice'] - base_price['baseprice'],
                                                                  2) if base_price is not None else customorder[
                                            'localprice']
                                        ex_rate = customorder['usdprice'] / customorder['localprice'] if customorder[
                                            'localprice'] else 0
                                        usd_delta_price = round(local_price_delta * ex_rate, 2)

                                        customorder['localprice'] = local_price_delta
                                        customorder['usdprice'] = usd_delta_price

                                    dollar['price_incl_tax'] = customorder['localprice']
                                    dollar['base_price_incl_tax'] = customorder['usdprice']
                                    dollar['tax_amount'] = customorder['localtaxamt']
                                    dollar['base_tax_amount'] = customorder['localtaxamt']
                                    dollar['order_currency_code'] = customorder['currencycode']
                                    dollar['invoice_date'] = customorder['invoicedate']

                    dollars.append(dollar)

                    p = re.compile(r'\bCUSTOM*\b', flags=re.I | re.X)
                    m = p.search(orderitem['ordersku'])
                    purchase_type = 'channel'

                    if m:
                        custom_rec = getcustomrecord(order['vouchers_serial'])
                        custom_redeem = iscustomredeemed(custom_rec[0]['id']) if custom_rec else 0
                        order['increment_id'] = None
                        order['sku'] = order['skus_id']
                        order['customer_id'] = order['customers_id']
                        order['created_at'] = orderitem['created_at']

                        #
                        if custom_rec and custom_redeem:
                            if dollar['base_price_incl_tax'] is None: dollar['base_price_incl_tax'] = custom_rec[0][
                                'asp']
                            if dollar['price_incl_tax'] is None: dollar['price_incl_tax'] = custom_rec[0]['asp'] * (
                                ex_rate if ex_rate is not None else 1)
                            issue_type = 'hw/sw_custom'
                            customorderrecord(order, dollars, purchase_type, issue_type)
                            buildcustomdata(order, product_catalog, dollars, conn1)
                        else:
                            issue_type = 'hw/sw_custom_notredeemed'
                            customorderrecord(order, dollars, purchase_type, issue_type)
                    else:
                        if orderitem['ordersku'].find('ULTIMATE') != -1:
                            issue_type = 'hw/sw_ultimate'
                        else:
                            issue_type = 'hw/sw_bundle'

                        builddata(orderitem, product_catalog, dollars, conn1, purchase_type, issue_type)


                # Tested part of NAMMB2B

                elif order['voucher_type'] == 'nammb2b':
                    with conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                        sql = """ select *, ih.invoicedate from rpt.salesmargindetail sm join erp.orderdtl od
                                    ON sm.ordernum = od.ordernum AND sm.orderline = od.orderline
                                    JOIN erp.invchead ih ON sm.ordernum = ih.ordernum
                                    where od.reference = %s
                                """
                        cursor.execute(sql, (order['namm_id'],))
                        nammrec = cursor.fetchone()

                        if nammrec:
                            dollars = list()
                            dollar = dict()
                            dollar['price_incl_tax'] = nammrec['localprice']
                            dollar['base_price_incl_tax'] = nammrec['usdprice']
                            dollar['tax_amount'] = nammrec['localtaxamt']
                            dollar['base_tax_amount'] = nammrec['localtaxamt']
                            dollar['discount_amount'] = dollar['base_discount_amount'] = dollar['voucher_amount'] = \
                                dollar['base_voucher_amount'] = 0
                            dollar['order_currency_code'] = nammrec['currencycode']
                            dollar['hw_serialnum'] = None
                            dollar['invoice_date'] = nammrec['invoicedate']
                            dollars.append(dollar)
                            purchase_type = 'channel'

                            order['entity_id'] = orderitem['orderid'] = nammrec['ordernum']
                            order['increment_id'] = orderitem['order_increment_id'] = order['namm_id']
                            order['item_id'] = orderitem['item_id'] = nammrec['orderline']

                            if not orderitem['ASPSkus']:
                                issue_type = 'nammb2b'
                                custom_rec = getcustomrecord(order['vouchers_serial'])
                                custom_redeem = iscustomredeemed(custom_rec[0]['id']) if custom_rec else 0
                                order['sku'] = order['skus_id']
                                order['customer_id'] = order['customers_id']

                                order['created_at'] = orderitem['created_at']
                                issue_type = 'nammb2b_custom'
                                customorderrecord(order, dollars, purchase_type, issue_type)
                                buildcustomdata(order, product_catalog, dollars, conn1)

                                # ENd of Testing block

                            else:
                                issue_type = 'nammb2b_'
                                builddata(orderitem, product_catalog, dollars, conn1, purchase_type, issue_type)

    # Process Credits
    process_channel_credit()
    nammb2b_credits()


'''
                else:
                    print(order['voucher_type'], orderitem)

            else:
                print("Voucher type not handled", order['voucher_type'])
                # print(order)
'''


def process_channel_credit():
    with conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        sql = """select distinct serialnumber
                    from rpt.serialnumberhistory
                  where trantype = 'RMA-ENTRY' AND trandate BETWEEN '2018-01-01' AND '2018-12-01' """
        cursor.execute(sql)
        credit_serial_nos = cursor.fetchall()
        credit_serials = [x['serialnumber'] for x in credit_serial_nos]

        for x in credit_serials:
                with conn1.cursor() as cursor:
                    sql = """INSERT INTO dev.royalty (select purchase_type, item_type, order_id,order_increment_id, item_id,
                             customer_id, order_sku, voucher_serial, custom_serial, sku, created_at, list_price, discounted_list_price, pro_rata, 
                             net_price * -1, base_net_price * -1, price_incl_tax * -1, base_price_incl_tax * -1, 
                             tax_amount * -1, base_tax_amount * -1, discount_amount * -1, base_discount_amount * -1, 
                             special_price * -1, base_special_price * -1, owner_discount_amount * -1, 
                             base_owner_discount_amount * -1, special_owner_discount_price * -1 , base_special_owner_discount_price * -1,
                             voucher_amount * -1, base_voucher_amount * -1, order_currency_code, custom_history_id,
                             custom_id, hw_serialnumber, invoice_date, order_state, order_status, 'Channel_Credit' from dev.royalty
                            where purchase_type = 'channel' AND item_type != 'hw/sw_custom' AND hw_serialnumber = %s
                            )"""
                    cursor.execute(sql, (x,))
                    conn1.commit()



def nammb2b_credits():
    with conn1.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        sql = """ select distinct order_increment_id as id from dev.royalty where item_type like 'namm%' 
                AND item_type != 'nammb2b-credit'
             """
        cursor.execute(sql, )
        namm_orders = cursor.fetchall()
        print(namm_orders)

    with conn.cursor() as cursor:
        sql = "select distinct id from uaudio.nammb2b where state = -1"
        cursor.execute(sql, )
        credit_orders = cursor.fetchall()
        credit_orders = [x['id'] for x in credit_orders]

    for x in namm_orders:
        if x['id'] in credit_orders:
            with conn1.cursor() as cursor:
                sql = """INSERT INTO dev.royalty (select purchase_type, 'nammb2b-credit', order_id,order_increment_id, item_id,
                         customer_id, order_sku, voucher_serial, custom_serial, sku, created_at, list_price, discounted_list_price, pro_rata, 
                         net_price * -1, base_net_price * -1, price_incl_tax * -1, base_price_incl_tax * -1, 
                         tax_amount * -1, base_tax_amount * -1, discount_amount * -1, base_discount_amount * -1, 
                         special_price * -1, base_special_price * -1, owner_discount_amount * -1, 
                         base_owner_discount_amount * -1, special_owner_discount_price * -1 , base_special_owner_discount_price * -1,
                         voucher_amount * -1, base_voucher_amount * -1, order_currency_code, custom_history_id,
                         custom_id, hw_serialnumber, invoice_date from dev.royalty r
                        where r.order_increment_id = %s
                        --AND r.item_type != 'nammb2b_custom'
                        )"""
                cursor.execute(sql, (x['id'],))
                conn1.commit()


if __name__ == '__main__':
    conn, conn1, conn2 = dbconnection()
    SkuMap = buildskumap()
    product_catalog = catalogproducts()
    discount_price= discountprice()
    # customswap()
    # vouchers = getorders()
    # processorders(vouchers, conn1, SkuMap, product_catalog)
    processcredits()
    # getchannelorders(product_catalog, SkuMap)
    # process_channel_credit()

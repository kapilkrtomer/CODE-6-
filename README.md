CODE-6-
=======
COMPLETE CODE FOR ZOVON


#!/usr/bin/python

import MySQLdb
import os 
import sys


class to_common(object):
    
    def __init__(self, dbname, table1, table2, t3, zovtable, zovtable2, sitename, seller_brand, directory, csvfile, csvfile2):
        self.directory = directory 
        self.dbname = dbname
	self.table1  = table1
	self.table2 = table2
	self.seller_brand = seller_brand
        self.csvfile = csvfile
        self.csvfile2 = csvfile2
        self.zovtable = zovtable
        self.zovtable2 = zovtable2
        self.sitename = sitename
        self.t3 = t3 

	changemode = "chown mysql:mysql %s" %(directory)
        os.system(changemode)
	
        #print  [dbname,table1,table2,zovtable,zovtable2,sitename,seller_brand,directory,csvfile]	   

	db = MySQLdb.connect("192.99.13.229","root","6Tresxcvbhy", dbname)
	cursor = db.cursor()

	self.db = db 
	self.cursor = cursor

        self.db2 = db2 = MySQLdb.connect("54.201.218.138","root","india@123", "zov", local_infile = 1)
        self.cursor2 = cursor2 = db2.cursor()



    def __del__(self):
        self.db.close()
        self.db2.close()



    def table_connect(self):
        cursor = self.cursor
        db = self.db 

        sql = """ truncate %s""" %(self.table2)

        try:
            cursor.execute(sql)
            db.commit()

        except:
            db.rollback()


        sql = """ truncate %s""" %(self.t3)

        try:
            cursor.execute(sql)
            db.commit()

        except:
            db.rollback()
    

        sql = """insert ignore  into %s ( SKU,SKU_Target, Title, Link, Price, Category, subCategory,ss_category, Brand, Image, ListPrice, \
                                 ColourName, targetAudience, productUrl, seller, metaTitle, metaDescription, size, log_discription, \
                                 log_specification, seller_brand )\
                                ( select product_id,CONCAT(product_id, "_", target) , product_title, product_url, selliing_price, category, \
                                  sub_category,ss_category, brand,sort_image_link,  mrp, color, target, product_title_zovon, \
                                  seller, meta_title, meta_desc, size,product_desc, product_spec, seller_brand from \
                                  %s where  upload_status = 'NO' and upload_image_status = "YES" and failing_status = "NO" 
                                  and  status = "A")"""
        sql = sql %(self.table2,  self.table1)
       # print sql
        try:
            cursor.execute(sql)
            db.commit()
            print "...........inserted..............."
        except:
            db.rollback()  

        sql = """update %s set upload_status  = "YES" """
        sql = sql %(self.table1)

        try:
            cursor.execute(sql)
            db.commit()

        except:
            db.rollback()



    def to_zovon(self):
        csvfile = self.csvfile
        sql = """ select SKU, Title, Link, Price, Category, subCategory, ss_category,  Brand, Image, ListPrice,\
                  ColourName, targetAudience, productUrl, seller, metaTitle, metaDescription,\
                  size, log_discription, log_specification, seller_brand  , Sku_Target \
                  from %s  where updated_just_now = "YES" INTO OUTFILE "%s" \
		  FIELDS ENCLOSED BY '"' TERMINATED BY ',' ESCAPED BY '\'
		  LINES TERMINATED BY '\r\r\r\r\n'; """

        sql = sql %(self.table2, self.csvfile)
        print sql

        try:
            self.cursor.execute(sql)
            self.db.commit()
        except:
            self.db.rollback()

        csvfile = self.csvfile2

        sql = """ select SKU, Title, Link, Price, Category, subCategory, ss_category,  Brand, Image, ListPrice,\
                  ColourName, targetAudience, productUrl, seller, metaTitle, metaDescription,\
                  size, log_discription, log_specification, seller_brand  , Sku_Target \
                  from %s  where updated_just_now = "YES" INTO OUTFILE "%s" \
                  FIELDS ENCLOSED BY '"' TERMINATED BY ',' ESCAPED BY '\'
                  LINES TERMINATED BY '\r\r\r\r\n'; """

        sql = sql %(self.t3, self.csvfile2)


        try:
            self.cursor.execute(sql)
            self.db.commit()
        except:
            self.db.rollback()

        



    def to_zovon_import(self, csvfile):

        sql = """CREATE TEMPORARY TABLE %s LIKE %s;""" %(self.table1, self.zovtable)
        try:
            self.cursor2.execute(sql)
            self.db2.commit()
        except:
            self.db2.rollback()

        
        
        sql = """LOAD DATA local INFILE "%s"  replace   INTO TABLE %s  FIELDS TERMINATED BY ',' ENCLOSED BY '"' \
                 ESCAPED BY '\' LINES TERMINATED BY '\r\r\r\r\n' \
                 ( SKU, Title, Link, Price, Category, subCategory, ss_category,Brand, Image, ListPrice,\
                   ColourName, targetAudience, productUrl, seller, metaTitle, metaDescription,\
                   size, log_discription, log_specification, seller_brand, Sku_Target);"""

        sql = sql %(csvfile, self.table1)

        try:
            self.cursor2.execute(sql)
            self.db2.commit()
        except:
            self.db2.rollback()

        table_match = {"your_table":self.zovtable, "your_temp_table":self.table1}

        sql = """insert into %(your_table)s ( SKU, Title, Link, Price, Category, subCategory, ss_category, Brand, Image, ListPrice,\
                                  ColourName, targetAudience, productUrl, seller, metaTitle, metaDescription,\
                                  size, log_discription, log_specification, seller_brand,Sku_Target\
                                ) \
                                ( select SKU, Title, Link, Price, Category, subCategory,ss_category, Brand, Image, ListPrice,\
                                  ColourName, targetAudience, productUrl, seller, metaTitle, metaDescription,\
                                  size, log_discription, log_specification, seller_brand,Sku_Target from %(your_temp_table)s
                                ) ON DUPLICATE KEY """
        sql = sql %(table_match)


 
        sql2 = """UPDATE \
               Price = %(your_temp_table)s.Price, \
               ListPrice = %(your_temp_table)s.ListPrice,\
               ColourName = %(your_temp_table)s.ColourName,\
               size = %(your_temp_table)s.size,\
               log_discription = %(your_temp_table)s.log_discription, \
               log_specification = %(your_temp_table)s.log_specification,\
               p_status ="NU"; """ %(table_match)

        sql = """%s%s""" %(sql, sql2)


        try:
            self.cursor2.execute(sql)
            self.db2.commit()
        except:
            self.db2.rollback()

        sql ="DROP TEMPORARY TABLE %(your_temp_table)s;" %(table_match)

        try:
            self.cursor2.execute(sql)
            self.db2.commit()
        except:
            self.db2.rollback()



    def pattern_matching(self):

        #sql = """SELECT target, category, sub_category, sub_category from %s WHERE upload_status = 'YES'\
        #                          and upload_image_status = "YES" \
        #                          and failing_status = "NO" and  status = "A" """

        #sql = sql %(self.table1)

        #self.cursor.execute(sql)
        #results2 = self.cursor.fetchall()

        #t_c_s_ssc_ptrn = ["+".join(list(t_c_s_ssc)) for t_c_s_ssc in results2]

        sql = """SELECT other_cat, zov_cat  FROM %s  WHERE site_name = "%s" and site_table = "%s" """ 
        sql = sql %(self.zovtable2, self.sitename, self.table1)

        self.cursor2.execute(sql)
        results = self.cursor2.fetchall()
        #print results    

        pattern_matching2 = self.pattern_matching2
        table2, table1 = self.table2, self.table1
        cursor, db = self.cursor, self.db

        pattern_dict = {}

        for ptrn in results:
            pattern_dict[ptrn[0]] = ptrn[1] 

        self.pattern_dict = pattern_dict


        info = [pattern_matching2(prtnset[0], prtnset[1], table2, table1, cursor, db ) for prtnset in results]

        del info[:]
        del info  



    def insert_n_update_empty_tble(self, main_ptrn, v):
       
        sql = """ insert  into %s  ( SKU, Title, Link, Price, Category, subCategory, ss_category,  Brand, Image, ListPrice,\
                                  ColourName, targetAudience, productUrl, seller, metaTitle, metaDescription,\
                                  size, log_discription, log_specification, seller_brand, Sku_Target, updated_just_now \
                                ) \
                                (select  SKU, Title, Link, Price, "%s", "%s", "%s", Brand, Image, ListPrice,\
                                  ColourName, "%s", productUrl, seller, metaTitle, metaDescription,\
                                  size, log_discription, log_specification, seller_brand, CONCAT(SKU, "_%s"), "YES"\
                                  from %s where targetAudience = "%s" and Category = "%s" and subCategory  ="%s" and  \
                                  ss_category =  "%s") """

        v2 = v.split("+")

        length = len(v2)
       # print length
        if length == 4:
            pass

        elif length == 3:
            v2.extend([" "])

        elif length == 2:
            v2.extend([" ", " "])

        elif length == 1:
            v2.extend([" ", " ", " "])

        else:
            return 0

        
        sql_tuple = tuple([self.t3, v2[1], v2[2], v2[3] , v2[0], v2[0], self.table2] + main_ptrn)
	sql = sql %(sql_tuple)
        #print sql

        try:
            self.cursor.execute(sql)
            self.db.commit()
        except:
            self.db.rollback()



    

  



    def pattern_matching2(self, k, v, table2, table1, cursor, db):
        #k = "men+clothing+men+outerwear"
        main_ptrn = [ln.strip() for ln in k.split("+")]
     
        #v = "women+panty+everyday+value packs@men+winter wear+winter wear+ wear"
        
        v2 = v.split("@")
        length = len(v2)
        #print length
         
        if length == 2:
            self.insert_n_update_empty_tble(main_ptrn, v2[1])
            v = v2[0]
        else:
            v = v2[0]

        subs_ptrn = [ln.strip().lower() for ln in v.split("+")]

        length = len(subs_ptrn)
        
        if length == 4:
	    pass

	elif length == 3:
            subs_ptrn.extend([" "])

	elif length == 2:
	    subs_ptrn.extend([" ", " "])

	elif length == 1:
	    subs_ptrn.extend([" ", " ", " "])

        else:
            return 0

	subs_ptrn.extend(main_ptrn)

        final_string = [table2, table1]
        final_string.extend(subs_ptrn)
        final_string = tuple(final_string)

        sql = """UPDATE %s t2 INNER JOIN %s t1 \
                 ON t1.product_url = t2.Link \
                 set t2.targetAudience = "%s", t2.Category = "%s", t2.subCategory = "%s" , t2.ss_category = "%s", t2.updated_just_now ="YES"\
		 where t1.target = "%s" and t1.category="%s" and  t1.sub_category="%s" and  t1.ss_category ="%s"; """
 

        sql = sql %(final_string)
        #print sql
        try:
	   cursor.execute(sql)
           db.commit()
           print "updated................................."
	except:
	    db.rollback()

        


    



def supermain():
    import time 
    dbname, table1, table2, t3, seller_brand = ("zivame", "zivame_data", "zivame_data_cmn",  "zivame_data_cmn2", "www.zivame.com")
    directory = "/home/desktop/anit/zivame/zivame_dir"
    csvfile = "%s/to_zovon%s.csv" %(directory, time.strftime("%d%m%y%H%M%S"))
    time.sleep(1)
    csvfile2 = "%s/to_zovon2%s.csv" %(directory, time.strftime("%d%m%y%H%M%S"))
    zovtable = "zovondetails_new"
    zovtable2 = "site_synonyms"
    sitename = "zivame"
    obj = to_common(dbname, table1, table2, t3, zovtable, zovtable2, sitename,  seller_brand, directory, csvfile, csvfile2)
    
    
    obj.table_connect()
    obj.pattern_matching()

    obj.to_zovon()
    obj.to_zovon_import(obj.csvfile)
    obj.to_zovon_import(obj.csvfile2)

    


    

    




if __name__=="__main__":
    supermain()

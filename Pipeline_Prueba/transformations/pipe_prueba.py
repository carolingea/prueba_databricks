from pyspark import pipelines as dp

# Alerta si hay PriceSale > 0
@dp.expect("valid_price", "PriceSale > 0")

# Quita los registros que tienen QuantitySale > 0
@dp.expect_or_drop("valid_quantity", "QuantitySale > 0")

@dp.table(name="ventas_silver")
def ventas():   
    idCustomer = spark.conf.get("idCustomer", "%")
    return spark.sql(f"""
        select 
            v.Id,
            v.Price as PriceSale, 
            v.Quantity as QuantitySale, 
            v.Total,
            p.Name as Product, p.Category, p.Brand, 
            p.Price as PriceProduct, 
            p.Currency, p.Stock, p.EAN, p.Color, p.Size, p.Availability, 
            c.`First Name` as FirstName,
            c.`Last Name` as LastName, 
            c.Company, c.City, c.Country, c.Email, 
            c.`Phone 1` as Phone1, 
            c.`Phone 2` as Phone2            
        FROM ventas v
        INNER JOIN workspace.default.products p on p.Index = v.productid
        INNER JOIN workspace.default.customers c on c.Index = v.customerid
        WHERE v.customerid like '{idCustomer}'
        """)

# Lee la ventas_silver y la asigna en la ventas_gold
@dp.table(name="ventas_gold")
def ventas_filtrado():
    return spark.read.table("ventas_silver")

#------------- Lee ventas gold como stream -------------------
# Filtrar los que esten en category Cycling, Furniture, Makeup
@dp.expect_or_drop("valid_total", "Category IN ('Cycling','Furniture' ,'Makeup')")

# Filtrar los que tengan Total negativo o cero
@dp.expect_or_drop("valid_quantity", "Total > 0")

@dp.table(name="ventas_gold_stream_filter")
def ventas_filtrado_stream():
    return spark.readStream.option("skipChangeCommits", "true").table("ventas_gold")

# ------------ Tabla de customers -------------
@dp.expect_or_drop("valid_customerid", "Country IN('Panama','Venezuela','Colombia','Brazil', 'Mexico', 'Ecuador')")
@dp.table(name="customers_tmp")
def products():
    return (spark.read.table("customers")
        .withColumnRenamed("Customer Id", "CustomerId")
        .withColumnRenamed("First Name", "FirstName")
        .withColumnRenamed("Last Name", "LastName")
        .withColumnRenamed("Phone 1", "Phone1")
        .withColumnRenamed("Phone 2", "Phone2")
        .withColumnRenamed("Subscription Date", "SubscriptionDate")
    )


# @dp.materialized_view()
# def ventas_clean():
#     return spark.sql("select * from workspace.default.ventas")

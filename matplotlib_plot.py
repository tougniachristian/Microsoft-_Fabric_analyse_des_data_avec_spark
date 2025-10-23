from matplotlib import pyplot as plt
sqlQuery = "SELECT CAST(YEAR(OrderDate) AS CHAR(4)) AS OrderYear, \
                SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue, \
                COUNT(DISTINCT SalesOrderNumber) AS YearlyCounts \
            FROM salesorders \
            GROUP BY CAST(YEAR(OrderDate) AS CHAR(4)) \
            ORDER BY OrderYear"
df_spark = spark.sql(sqlQuery)
df_sales = df_spark.toPandas()

plt.clf()
fig, ax = plt.subplots(1, 2, figsize=(10,4))
ax[0].bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')
ax[0].set_title('Revenue by Year')
ax[1].pie(df_sales['YearlyCounts'], labels=df_sales['OrderYear'])
ax[1].set_title('Orders per Year')
fig.suptitle('Sales Data')
plt.show()
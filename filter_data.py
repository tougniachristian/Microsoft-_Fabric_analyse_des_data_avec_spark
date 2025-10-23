customers = df.select("CustomerName", "Email").where(df['Item'] == 'Road-250 Red, 52')
print(customers.count())
print(customers.distinct().count())
display(customers.distinct())
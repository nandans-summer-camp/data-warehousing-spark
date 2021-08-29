from operator import add

def sales_per_order(order):
    per_item = lambda o: o.price_each * o.quantity_ordered
    return sum(map(per_item, order.line_items))

orders.rdd.map(sales_per_order).reduce(add)

orders.rdd \
      .flatMap(lambda x: x.line_items) \
      .map(lambda x: x.price_each * x.quantity_ordered) \
      .reduce(add)

orders.rdd.map(lambda x: (x.customer.country, x)) \
          .mapValues(sales_per_order) \
          .reduceByKey(add) \
          .collectAsMap()

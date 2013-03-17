using Raven.Client.Indexes;
using System.Linq;
using System;
using System.Collections.Generic;
using Raven.Abstractions.Indexing;

namespace RavenDBMapReduceDemo
{
    /// <summary>
    /// related document index, allows Orders to be queried by name of related Customer
    /// </summary>
    public class OrdersByCustomerNameIndex : AbstractIndexCreationTask<Order>
    {
        public OrdersByCustomerNameIndex()
        {
            Map = Orders => from order in Orders
                            select new
                            {
                                CustomerId = order.CustomerId,
                                CustomerName = LoadDocument<Customer>(order.CustomerId).Name
                            };
        }
    } 
}

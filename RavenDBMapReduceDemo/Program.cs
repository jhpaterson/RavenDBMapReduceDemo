using Raven.Client;
using Raven.Client.Document;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RavenDBMapReduceDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            // initialise document store 
            // requires server running on localhost:default port, run Raven.Server.exe in packages/RavenDB.Server.2.XXXX/tools
            var store = new DocumentStore { Url = "http://localhost:8080" };
            store.Initialize();

            // create an index - this is our MapReduce index
            Raven.Client.Indexes.IndexCreation.CreateIndexes(typeof(ProductsOrderedIndex).Assembly, store);

            #region CREATE DOCUMENTS
            // NOTE: Order is aggregate, Product is denormalised within each OrderLine, Customer is referenced

            //Customer cust1 = new Customer { Id = "CUST1", Name = "Fernando", Address = "1 First Street" };
            //Customer cust2 = new Customer { Id = "CUST2", Name = "Felipe", Address = "2 Second Street" };
            //Product pr1 = new Product { Id = "PROD1", Description = "Baked Beans 250g", UnitPrice = 0.45M };
            //Product pr2 = new Product { Id = "PROD2", Description = "Cornflakes 750g", UnitPrice = 2.99M };
            //Product pr3 = new Product { Id = "PROD3", Description = "White loaf 800g", UnitPrice = 1.25M };
            //Product pr4 = new Product { Id = "PROD4", Description = "Beef mince 450g", UnitPrice = 2.29M };

            //Order ord1 = new Order
            //{
            //    Id = "ORD1",
            //    OrderDate = new DateTime(2013, 2, 27),
            //    CustomerId = cust1.Id,
            //    OrderLines = new List<OrderLine>{
            //        new OrderLine{Product = pr1,  Quantity = 5},
            //        new OrderLine{Product = pr2, Quantity = 8},
            //        new OrderLine{Product = pr3, Quantity = 2}
            //    }
            //};

            //Order ord2 = new Order
            //{
            //    Id = "ORD2",
            //    OrderDate = new DateTime(2013, 3, 2),
            //    CustomerId = cust2.Id,
            //    OrderLines = new List<OrderLine>{
            //        new OrderLine{Product = pr1, Quantity = 24},
            //        new OrderLine{Product = pr2, Quantity = 12}
            //    }
            //};

            //Order ord3 = new Order
            //{
            //    Id = "ORD3",
            //    OrderDate = new DateTime(2013, 3, 12),
            //    CustomerId = cust1.Id,
            //    OrderLines = new List<OrderLine>{
            //        new OrderLine{Product = pr2, Quantity = 6},
            //        new OrderLine{Product = pr3, Quantity = 3},
            //        new OrderLine{Product = pr4, Quantity = 4}
            //    }
            //};


            //using (IDocumentSession session = store.OpenSession())
            //{
            //    session.Store(cust1);
            //    session.Store(cust2);

            //    session.Store(pr1);
            //    session.Store(pr2);
            //    session.Store(pr3);

            //    session.Store(ord1);
            //    session.Store(ord2);
            //    session.Store(ord3);

            //    session.SaveChanges();
            //}
            #endregion CREATE

            #region QUERY
            using (IDocumentSession session = store.OpenSession())
            {
                // load document by Id
                var doc = session.Load<Customer>("CUST1");

                Console.WriteLine("LOADED CUSTOMER {0}", doc.Name);


                // simple query, will use index if suitable one exists, create dynamic index otherwise
                var target = "PROD3";
                var query1 = session.Query<Order>()
                                .Where(o => o.OrderLines.Any(i => i.Product.Id == target))
                                .ToList();

                Console.WriteLine("\nORDERS WHICH CONTAIN {0}", target);
                foreach (var result in query1)
                {
                    Console.WriteLine("OrderId: {0}", result.Id);
                }

                // query using MapReduce index, get quantity of each Product from all Orders combined
                // specify result type and index class in call
                var query2 = session.Query<ProductsOrderedResult, ProductsOrderedIndex>()
                    .ToList();

                Console.WriteLine("\nQUANTITY ORDERED PER PRODUCT");
                foreach (var result in query2)
                {
                    Console.WriteLine("Product: {0}, Quantity ordered: {1}", result.ProductId, result.Count);
                }

                // query using related document index - i.e. a "join", query Orders by name (not Id) of related Customer
                target = "Fernando";
                var query3 = session.Advanced.LuceneQuery<Order>("OrdersByCustomerNameIndex")
                    .WhereEquals("CustomerName", target).ToList();

                Console.WriteLine("\nORDERS FOR CUSTOMER NAME {0}", target);
                foreach (var result in query3)
                {
                    Console.WriteLine("OrderID: {0}, Order date: {1:D}", result.Id, result.OrderDate);
                }

                // query which loads referenced document
                var targetDate = new DateTime(2013, 2, 28);
                var query5 = session.Query<Order>()
                            .Customize(x => x.Include<Order>(o => o.CustomerId))
                            .Where(o => o.OrderDate > targetDate)
                            .ToList();

                Console.WriteLine("\nCUSTOMERS FOR ORDERS SINCE {0:D}", targetDate);
                foreach (var result in query5)
                {
                    // this will not require querying the server
                    var cust = session.Load<Customer>(result.CustomerId);
                    Console.WriteLine("{0}", cust.Name);
                }
            }
            #endregion QUERY

            Console.ReadLine();

        }
    }
}

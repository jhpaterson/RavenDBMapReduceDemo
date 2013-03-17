using Raven.Client.Indexes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RavenDBMapReduceDemo
{ 
    /// <summary>
    /// MapReduce index, gives quantity of each Product from all Orders combined
    /// </summary>
    public class ProductsOrderedIndex : AbstractIndexCreationTask<Order, 
        ProductsOrderedResult>
    {
        public ProductsOrderedIndex()
        {
            Map = orders => from ord in orders
                            from line in ord.OrderLines
                            select new
                            {
                                ProductId = line.Product.Id,
                                Count = line.Quantity
                            };
            Reduce = results => from result in results
                                group result by result.ProductId into g
                                select new
                                {
                                    ProductId = g.Key,
                                    Count = g.Sum(x => x.Count)
                                };
        }
    }

    /// <summary>
    /// result type for MapReduce index
    /// </summary>
    public class ProductsOrderedResult
    {
        public string ProductId { get; set; }
        public int Count { get; set; }
    }
}

using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;

namespace ALE.SimpeFlow
{
    class Program
    {
        static void Preparation()
        {
            //Recreate database if it doesn't exist
            SqlConnectionManager masterConnection = new SqlConnectionManager("Data Source=.;Integrated Security=false;User=sa;password=reallyStrongPwd123");
            DropDatabaseTask.DropIfExists(masterConnection, "demo");
            CreateDatabaseTask.Create(masterConnection, "demo");
            SqlConnectionManager dbConnection = new SqlConnectionManager("Data Source=.;Initial Catalog=demo;Integrated Security=false;User=sa;password=reallyStrongPwd123");

            //Create destination table
            CreateTableTask.Create(dbConnection, "OrderTable", new List<TableColumn>()
            { 
                new TableColumn("Id", "INT", allowNulls:false, isPrimaryKey:true, isIdentity:true),
                new TableColumn("Item", "NVARCHAR(50)"),
                new TableColumn("Quantity", "INT"),
                new TableColumn("Price", "DECIMAL(10,2)")
            });
        }

        static void Main(string[] args)
        {
            //Set up database 
            Preparation();

            //Create connection manager to newly created database
            SqlConnectionManager connMan = new SqlConnectionManager("Data Source=.;Initial Catalog=demo;Integrated Security=false;User=sa;password=reallyStrongPwd123");

            //Define the dataflow
            CsvSource<string[]> source = new CsvSource<string[]>("demodata.csv");

            RowTransformation<string[], Order> rowTrans = new RowTransformation<string[], Order>(
              row => new Order()
              {
                  Item = row[1],
                  Quantity = int.Parse(row[2]) + int.Parse(row[3]),
                  Price = int.Parse(row[4]) / 100
              });

            DbDestination<Order> dest = new DbDestination<Order>(connMan, "OrderTable");

            //Link & run flow
            source.LinkTo(rowTrans);
            rowTrans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }
    }
}

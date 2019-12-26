﻿using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace ALE.ComplexFlow {
    public class PrepareDb {

        TableDefinition OrderDataTableDef = new TableDefinition("demo.Orders",
            new List<TableColumn>() {
                new TableColumn("OrderKey", "int",allowNulls: false, isPrimaryKey:true, isIdentity:true),
                new TableColumn("Number","nvarchar(100)", allowNulls: false),
                new TableColumn("Item","nvarchar(200)", allowNulls: false),
                new TableColumn("Amount","money", allowNulls: false),
                new TableColumn("CustomerKey","int", allowNulls: false)
            });

        TableDefinition CustomerTableDef = new TableDefinition("demo.Customer",
            new List<TableColumn>() {
                new TableColumn("CustomerKey", "int",allowNulls: false, isPrimaryKey:true, isIdentity:true),
                new TableColumn("Name","nvarchar(200)", allowNulls: false),
            });

        TableDefinition CustomerRatingTableDef = new TableDefinition("demo.CustomerRating",
           new List<TableColumn>() {
               new TableColumn("RatingKey", "int",allowNulls: false, isPrimaryKey:true, isIdentity:true),
                new TableColumn("CustomerKey", "int",allowNulls: false),
                new TableColumn("TotalAmount","decimal(10,2)", allowNulls: false),
                new TableColumn("Rating","nvarchar(3)", allowNulls: false)
           });


        public void Prepare(ConnectionString connectionString) {
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(connectionString.GetMasterConnection());
            DropDatabaseTask.DropIfExists("demo");
            CreateDatabaseTask.Create("demo");
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(connectionString);
            CreateSchemaTask.Create("demo");
            OrderDataTableDef.CreateTable();
            CustomerTableDef.CreateTable();
            CustomerRatingTableDef.CreateTable();
            SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO demo.Customer values('Sandra Kettler')");
            SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO demo.Customer values('Nick Thiemann')");
            SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO demo.Customer values('Zoe Rehbein')");
            SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO demo.Customer values('Margit Gries')");

            CreateLogTablesTask.CreateLog();
            ControlFlow.STAGE = "Staging";
        }
    }
}
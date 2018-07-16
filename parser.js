var request = require("request");
var http = require("http");
var parse = require('xml-parser');
http.post = require('http-post');
var moment = require('moment');
var date = moment(Date.now()).format('YYYY-MM-DD H:m:s');
const util = require('util');
var fs = require("fs");
var parseString = require('xml2js').parseString;
const sql = require("mssql");
var dateTime = moment(Date.now()).format('YYYY-MM-DD H:m:s');
var date = moment(Date.now()).format('YYYY-MM-DD');
var data = '';
var member_cnt = 0;

// test server 
var api_url = 'http://api_url/test_xml_response';
var config = {
    user: '###',
    password: '###',
    server: 'ip-address',
    database: 'dbname',
	parseJSON : true 
};


(async function () {
	let pool = await sql.connect(config)
	var sql_query ="select  ltrim( rtrim(un_number))un_number from user_number join user_info on user_number.ui_user_id = user_info.ui_user_id where user_info.ui_user_type='agent' and un_number not like '%-%'";
	var result = await pool.request().query(sql_query)
	agentNumbers = result.recordset.map(i=>i.un_number).join(',');

	try {
		http.post(api_url, 
		{ KeyCode:'A46BD809-4659-4529-B9A7-B9DA4A523E4B', UserNumbers:agentNumbers, TransactionDate:date  }, 
			function(res){ 
				res.setEncoding('utf8');
				res.on('data', function(data_) { data += data_.toString(); });
				res.on('end', function() {
						var chunk = data.replace(/[\n\r]+/g,"");
						var cnt = 0;
						parseString(chunk,function (err, result) {
									
									var obj2 = result.ArrayOfTBMMTransactionSummary;
									var arrOfVals = obj2.TBMMTransactionSummary;
									arrOfVals.forEach(function(value){
								 
									var user_number = value.UserNumber[0];
									var total_cash = parseFloat(value.CurrentBalance[0]);
									var debit_count = value.TotalNoOfDebitTransaction[0];
									var credit_count = value.TotalNoOfCreditTransaction[0];
									var total_tnx_count = 0;
									var hhr_last_id;
									var tt_last_id;
									
									if(credit_count != null && debit_count != null){
										total_tnx_count = +debit_count + +credit_count;
									}
							
									var total_debit = parseFloat(value.TotalDebitAmount[0]);
									var total_credit = parseFloat(value.TotalCreditAmount[0]);
									cnt = 1+cnt;
							//console.log ('user number '+ cnt + ':'+user_number);
							
							if(!isNaN(total_cash)) {
								
								(async function () {
										try {
												var qry = `SELECT  ui_user_id  FROM user_number where un_number='`+user_number+ `'`;
												
												let result1 = await pool.request()
												   .query(qry)
												  
												//console.log (result1.rowsAffected);
												if (result1.rowsAffected > 0)
												{	
												
													var user_id = result1.recordset[0]['ui_user_id'];
													//member_cnt = member_cnt+1;
													//console.log ( "Member count : " + member_cnt + " - User Id : "+ user_id  );	
													
													qry2='SELECT TOP 1 * FROM half_hourly_request where ui_user_id = ' + user_id + ' ORDER BY created_at DESC'
													//console.log (qry2);	
													let result2 = await pool.request()
														.query(qry2)

													//console.dir (result2);

													/*if (value.ListOfTrnDetails)
                                						{	
                                    						var details = value.ListOfTrnDetails;
															//console.log(details);
															console.log(util.inspect(details, {depth: null}));
														}*/
														
													if (result2.rowsAffected > 0)
													{
														var hhr_digital_cash = parseFloat(result2.recordset[0]['hhr_digital_cash']);
														var hhr_debit_cash = parseFloat(result2.recordset[0]['hhr_totalDr']);
														var hhr_credit_cash = parseFloat(result2.recordset[0]['hhr_totalCr']);
														//console.log('UserId:'+ user_id+' Digital:' + hhr_digital_cash);
														

														if(hhr_debit_cash == total_debit ){ total_debit = 0; }
												 		if(hhr_credit_cash == total_credit ){ total_credit = 0; }
														if(Number((hhr_digital_cash).toFixed(2)) != Number((total_cash).toFixed(2)) && Number((total_cash).toFixed(2)) != ''){
												 				var recordsets = await pool.request().query("INSERT INTO [dbo].[half_hourly_request] ([ui_user_id],[hhr_digital_cash],[hhr_totalDr],[hhr_totalCr],[hhr_trans_count],[created_at],[updated_at]) VALUES ("+user_id+","+total_cash+","+total_debit+","+total_credit+","+total_tnx_count+",'"+dateTime+"','"+dateTime+"') SELECT SCOPE_IDENTITY() as last_id");
												 				recordsets.recordset.forEach(function (value_last_id) {
																		//console.log(value_last_id);
																		hhr_last_id = value_last_id['last_id'];
                                                    					// transaction details from  
                                                    					if (value.ListOfTrnDetails)
                                											{	
                                    											var details = value.ListOfTrnDetails;
																				//console.log(details);
																				console.log(util.inspect(details, {depth: null}));
																				if(details!=undefined){
																					details.forEach(function(hhr_details){ 

																							var details2 = hhr_details.TrnDetails;
																							console.log("DETAIL2 "+util.inspect(details2))
																							if(details2!=undefined){
																								details2.forEach(function(hhr_details2){
																									var details3 = hhr_details2;
								                                                                    var hhr_transaction_type = details3.TransactionType[0];
								                                                                    var hhr_debit = details3.TotalNoOfDebitTransection[0];
								                                                                    var hhr_debit_amount = details3.TotalDebitAmount[0];
								                                                                    var hhr_credit = details3.TotalNoOfCreditTransection[0];
								                                                                    var hhr_credit_amount = details3.TotalCreditAmount[0];
								                                                                   	var hhr_commission = details3.TotalCommissionAmount[0];                                     
								                                                                   	//console.log("TXN TYPE : "+hhr_transaction_type+"")
								                                                                    //console.log("HHR DEBIT : "+hhr_debit_amount+"")
								                                                                    //console.log("HHR CREDIT : "+hhr_credit_amount+"")
								                                                                    //console.log("TOTAL HHR COUNT : "+(+hhr_debit+ +hhr_credit)+"\n\n\n")
								                                                                    if(hhr_last_id != 0){
								                                                                    	var query3 = "select [tt_id],[tt_name] from [cmdss_db].[dbo].[transaction_type] where [tt_name] like '"+hhr_transaction_type+"'";
								                                                                    	

								                                   										pool.request() // or: new sql.Request(pool1)
								                                                                    	.query(query3, (err, tt_data) => {
								                                                                    		// ... error checks
								                                                                    		console.log ('err' + err);
								                                                                    		console.log(util.inspect (tt_data));
								                                                                    		tt_data.recordset.forEach(function (tt_value) {
									                                                                    		if(tt_value['tt_name'])
									                                                                    		{		
	                                                                                    								try {
									                                                                    					
	                                                                                    									var query4 = "INSERT INTO [dbo].[hhr_details] ([hhr_id],[tt_id],[hd_debit],[hd_debit_amount],[hd_credit],[hd_credit_amount],[hd_commission],[uoi_date],[created_at],[updated_at]) VALUES ("+hhr_last_id+","+tt_value['tt_id']+","+hhr_debit+","+hhr_debit_amount+","+hhr_credit+",'"+hhr_credit_amount+"','"+hhr_commission+"','"+dateTime+"','"+dateTime+"','"+dateTime+"') SELECT SCOPE_IDENTITY() as last_id"				
									                                                                    					console.log (query4);

									                                                                    					pool.request() // or: new sql.Request(pool1)
								                                                                    						.query(query4, (err, recordset) => {
								                                                                    							console.log (err)
								                                                                    							console.log('hd Affected: ' + util.inspect(recordset));			

								                                                                    						})	// end of query 4
									                                                                    				  // pool.request().query("INSERT INTO [dbo].[hhr_details] ([hhr_id],[tt_id],[hd_debit],[hd_debit_amount],[hd_credit],[hd_credit_amount],[hd_commission],[uoi_date],[created_at],[updated_at]) VALUES ("+hhr_last_id+","+tt_value['tt_id']+","+hhr_debit+","+hhr_debit_amount+","+hhr_credit+",'"+hhr_credit_amount+"','"+hhr_commission+"','"+dateTime+"','"+dateTime+"','"+dateTime+"') SELECT SCOPE_IDENTITY() as last_id")
	           																																				
	                                                                                    								}catch(err) {
	                                                                                        								console.log('hd Request error: ' + err);
	                                                                                    								}
	                                                                                							} // end of tt name 
								                                                                    	
								                                                                    		}); // end of recordset	


								                                                                    	}) // end of query3



								                                                                    }
																								}); // end of foreach details2

																							}// end of details2 


																					}); // end of foreach 
																				}// end of if details check




																			}// end of xml details	


                                                    					
																}); // end of recordset


												 		} // end of if 


																	
													}// end of hhr query result exist #result2
													else {
														 
														 var recordset = await pool.request().query("INSERT INTO [dbo].[half_hourly_request] ([ui_user_id],[hhr_digital_cash],[hhr_totalDr],[hhr_totalCr],[hhr_trans_count],[created_at],[updated_at]) VALUES ("+user_id+","+total_cash+","+total_debit+","+total_credit+","+total_tnx_count+",'"+dateTime+"','"+dateTime+"') SELECT SCOPE_IDENTITY() as last_id");
                                        				 recordset.recordset.forEach(function (value_last_id) {
                                        				 hhr_last_id = value_last_id['last_id'];
                                            				var details = value.ListOfTrnDetails;
                                            			 });

                                            			 // transaction details from  
                                                    					if (value.ListOfTrnDetails)
                                											{	
                                    											var details = value.ListOfTrnDetails;
																				//console.log(details);
																				console.log(util.inspect(details, {depth: null}));
																				if(details!=''){
																					details.forEach(function(hhr_details){ 

																							var details2 = hhr_details.TrnDetails;
																							console.log("DETAIL2 "+util.inspect(details2))

																							if(details2!=undefined){
																								
																								details2.forEach(function(hhr_details2){
																									var details3 = hhr_details2;
																									console.log("DETAIL3 "+util.inspect(details3))

																									console.log (details3.TotalNoOfDebitTransection[0]);
								                                                                    var hhr_transaction_type = details3.TransactionType[0];
								                                                                    var hhr_debit = details3.TotalNoOfDebitTransection[0];
								                                                                    var hhr_debit_amount = details3.TotalDebitAmount[0];
								                                                                    var hhr_credit = details3.TotalNoOfCreditTransection[0];
								                                                                    var hhr_credit_amount = details3.TotalCreditAmount[0];
								                                                                   	var hhr_commission = details3.TotalCommissionAmount[0];
								                                                                    console.log("TXN TYPE : "+hhr_transaction_type+"")
								                                                                    console.log("HHR DEBIT : "+hhr_debit_amount+"")
								                                                                    console.log("HHR CREDIT : "+hhr_credit_amount+"")
								                                                                    console.log("TOTAL HHR COUNT : "+(+hhr_debit+ +hhr_credit)+"\n\n\n")
								                                                                    //console.log (" last id : "+ hhr_last_id);

								                                                                    if(hhr_last_id != 0){
								                                                                    	
								                                                                    	var query3 = "select [tt_id],[tt_name] from [cmdss_db].[dbo].[transaction_type] where [tt_name] like '"+hhr_transaction_type+"'";
								                                                                    	//var tt_data =   pool.request().query(query3)	
								                                                                    	pool.request() // or: new sql.Request(pool1)
								                                                                    	.query(query3, (err, tt_data) => {
								                                                                    		// ... error checks
								                                                                    		console.log ('err' + err);
								                                                                    		console.log(util.inspect (tt_data));
								                                                                    		tt_data.recordset.forEach(function (tt_value) {
									                                                                    		if(tt_value['tt_name'])
									                                                                    		{		
	                                                                                    								try {
									                                                                    					//var recordset =  pool.request().query("INSERT INTO [dbo].[hhr_details] ([hhr_id],[tt_id],[hd_debit],[hd_debit_amount],[hd_credit],[hd_credit_amount],[hd_commission],[uoi_date],[created_at],[updated_at]) VALUES ("+hhr_last_id+","+tt_value['tt_id']+","+hhr_debit+","+hhr_debit_amount+","+hhr_credit+",'"+hhr_credit_amount+"','"+hhr_commission+"','"+dateTime+"','"+dateTime+"','"+dateTime+"') SELECT SCOPE_IDENTITY() as last_id")
	           																												//console.log('hd Affected: ' + recordset);									
	                                                                                    									var query4 = "INSERT INTO [dbo].[hhr_details] ([hhr_id],[tt_id],[hd_debit],[hd_debit_amount],[hd_credit],[hd_credit_amount],[hd_commission],[uoi_date],[created_at],[updated_at]) VALUES ("+hhr_last_id+","+tt_value['tt_id']+","+hhr_debit+","+hhr_debit_amount+","+hhr_credit+",'"+hhr_credit_amount+"','"+hhr_commission+"','"+dateTime+"','"+dateTime+"','"+dateTime+"') SELECT SCOPE_IDENTITY() as last_id"				
									                                                                    					console.log (query4);

									                                                                    					pool.request() // or: new sql.Request(pool1)
								                                                                    						.query(query4, (err, recordset) => {
									                                                                    						console.log (err)
									                                                                    						console.log('hd Affected: ' + util.inspect(recordset));			

								                                                                    						})	// end of query 4


	                                                                                    								}catch(err) {
	                                                                                        								console.log('hd Request error: ' + err);
	                                                                                    								}
	                                                                                							} // end of tt name 
								                                                                    	
								                                                                    		}); // end of recordset	


								                                                                    	}) // end of query 3




								                                                                    	
								                                                                    	 	

								                                                                    }
																								}); // end of foreach details2

																							}// end of details2 


																					}); // end of foreach 
																				}// end of if details check




																			}// end of xml details



													} // end of hhr query result not exist #result2

													
												
												}
											} catch (err) {
												console.log ("error : "+user_number+" :" + err);
												
												// ... error checks 
											}
											
											
								})() // end of async	
								
								
								
							}// end of total  cash 	
							
							 
						
					}); // end of user number  loop 
									
							
				}); // end of parse string

						

				});// end of res end function 
			});// end of http post 	
		}catch (err) {

			console.log (err)

		} // end of catch1

})() // end of async function 	

return;

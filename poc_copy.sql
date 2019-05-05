\timing
drop table af_poc_tmp;             
create table af_poc_tmp with (appendonly=true,compresslevel=5,orientation=column)
 as select * from af_poc_ext       
distributed by (CAPTURE_ID);       
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '0 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__1 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '1 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__2 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '2 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__3 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '3 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__4 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '4 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__5 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '5 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__6 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '6 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__7 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '7 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__8 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '8 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__9 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '9 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__10 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '10 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__11 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '11 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__12 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '12 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__13 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '13 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__14 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '14 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__15 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '15 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__16 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '16 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__17 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '17 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__18 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '18 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__19 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '19 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__20 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '20 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__21 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '21 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__22 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '22 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__23 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '23 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__24 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '24 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__25 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '25 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__26 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '26 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__27 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '27 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__28 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '28 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__29 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '29 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__30 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '30 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__31 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '31 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__32 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '32 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__33 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '33 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__34 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '34 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__35 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '35 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__36 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '36 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__37 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '37 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__38 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '38 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__39 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '39 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__40 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '40 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__41 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '41 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__42 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '42 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__43 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '43 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__44 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '44 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__45 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '45 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__46 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '46 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__47 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '47 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__48 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '48 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__49 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '49 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__50 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '50 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__51 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '51 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__52 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '52 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__53 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '53 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__54 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '54 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__55 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '55 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__56 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '56 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__57 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '57 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__58 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '58 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__59 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '59 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__60 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '60 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__61 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '61 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__62 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '62 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__63 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '63 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__64 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '64 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__65 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '65 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__66 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '66 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__67 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '67 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__68 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '68 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__69 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '69 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__70 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '70 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__71 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '71 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__72 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '72 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__73 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '73 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__74 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '74 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__75 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '75 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__76 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '76 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__77 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '77 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__78 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '78 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__79 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '79 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__80 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '80 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__81 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '81 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__82 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '82 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__83 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '83 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__84 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '84 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__85 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '85 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__86 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '86 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__87 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '87 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__88 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '88 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__89 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '89 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__90 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '90 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__91 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '91 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__92 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '92 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__93 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '93 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__94 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '94 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__95 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '95 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__96 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '96 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__97 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '97 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__98 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '98 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__99 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '99 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__100 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '100 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__101 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '101 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__102 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '102 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__103 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '103 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__104 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '104 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__105 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '105 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__106 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '106 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__107 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '107 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__108 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '108 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__109 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '109 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__110 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '110 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__111 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '111 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__112 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '112 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__113 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '113 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__114 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '114 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__115 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '115 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__116 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '116 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__117 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '117 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__118 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '118 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__119 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '119 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__120 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '120 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__121 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '121 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__122 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '122 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__123 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '123 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__124 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '124 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__125 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '125 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__126 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '126 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__127 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '127 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__128 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '128 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__129 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '129 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__130 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '130 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__131 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '131 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__132 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '132 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__133 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '133 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__134 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '134 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__135 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '135 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__136 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '136 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__137 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '137 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__138 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '138 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__139 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '139 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__140 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '140 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__141 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '141 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__142 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '142 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__143 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '143 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__144 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '144 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__145 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '145 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__146 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '146 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__147 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '147 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__148 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '148 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__149 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '149 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__150 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '150 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__151 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '151 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__152 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '152 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__153 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '153 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__154 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '154 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__155 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '155 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__156 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '156 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__157 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '157 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__158 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '158 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__159 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '159 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__160 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '160 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__161 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '161 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__162 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '162 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__163 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '163 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__164 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '164 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__165 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '165 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__166 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '166 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__167 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '167 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__168 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '168 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__169 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '169 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__170 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '170 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__171 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '171 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__172 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '172 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__173 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '173 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__174 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '174 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__175 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '175 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__176 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '176 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__177 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '177 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__178 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '178 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__179 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '179 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__180 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '180 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__181 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '181 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__182 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '182 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__183 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '183 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__184 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '184 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__185 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '185 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__186 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '186 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__187 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '187 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__188 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '188 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__189 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '189 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__190 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '190 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__191 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '191 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__192 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '192 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__193 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '193 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__194 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '194 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__195 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '195 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__196 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '196 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__197 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '197 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__198 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '198 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__199 with table af_poc_tmp1;
drop table af_poc_tmp1;            
create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as 
select                             
      CAPTURE_ID                   
     ,EG_CAPTURE_ID                
     ,CAPTURE_SEQ                  
     ,CAPTURE_TIME + interval '199 day' as CAPTURE_TIME 
     ,DEVICE_ID                    
     ,DEVICE_NAME                  
     ,LNG                          
     ,LAT                          
     ,QUALITY_SCORE                
     ,AGE                          
     ,AGE_GROUP                    
     ,GENDER                       
     ,FACE_TOTAL_SCORE             
     ,LIGHT_SCORE                  
     ,MASK_SCORE                   
     ,MASK_STATE                   
     ,CLARITY_SCORE                
     ,GLASSES_SCORE                
     ,GLASSES_STATE                
     ,MOUTH_SCORE                  
     ,FACE_CLARITY_SCORE           
     ,SUN_GLASSES_STATE            
     ,SUN_GLASSES_SCORE            
     ,NATION                       
     ,PITCH                        
     ,YAW                          
     ,ROLL                         
     ,X                            
     ,Y                            
     ,WIDTH                        
     ,HEIGHT                       
     ,PRINT_FACE_SCORE             
     ,LEFT_BLINK_SCORE             
     ,RIGHT_BLINK_SCORE            
     ,LEFT_BLINK_STATE             
     ,RIGHT_BLINK_STATE            
     ,IS_LINKAGE                   
     ,FEATURE_STATUS               
     ,CREATE_TIME                  
     ,CAPTURE_RECEIVE_TIME         
     ,ENGINE_CALL_TIME             
     ,YEAR                         
     ,MONTH                        
     ,DAY                          
 from af_poc_tmp                   
 distributed by (CAPTURE_ID)       
;      
alter table af_poc_20b exchange partition pn__200 with table af_poc_tmp1;

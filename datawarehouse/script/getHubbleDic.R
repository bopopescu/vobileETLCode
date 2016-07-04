library(RMySQL)
library(xlsx)

db <- "p2presult"
uname <- "report"
passwd <- "report"
host <- "p2p-2a.c85gtgxi0qgc.us-west-1.rds.amazonaws.com"
file_path <- "/tmp/hubble-p2presult-dic.xlsx"

# db <- "vobile"
# uname <- "root"
# passwd <- "test"
# host <- "192.168.1.40"

conn <- dbConnect(MySQL(), dbname = db, username=uname, host=host, password=passwd)
table_list <- dbGetQuery(conn = conn, statement = "show tables")
data_tmp2 <- ""
for (table_name in table_list[, 1]){
  data <- dbGetQuery(conn = conn, statement = paste("select COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT, COLUMN_COMMENT, EXTRA from information_schema.COLUMNS where TABLE_SCHEMA = '", db,"' and TABLE_NAME = '", table_name, "'", sep = ""))
    
  data_tmp1 <- rbind2(names(data), data)
  data_tmp1 <- rbind2(table_name, data_tmp1)
  data_tmp1 <- rbind2(data_tmp1, "")
  data_tmp2 <- rbind2(data_tmp1, data_tmp2)
}
write.xlsx2(x = data.frame(data_tmp2), file = file_path, sep = "", 
            sheetName = "Sheet1", col.names = T, append = T, row.names = F)

dbDisconnect(conn)

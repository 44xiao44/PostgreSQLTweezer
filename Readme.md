### NAME
PostgreSQLTweezer - A tool to get error logs and slow sql logs from database log files

### Arguments:

    logfile can be a single log file ，or a single folder where the logs are stored

### Options:

    -h          | --help           : Get help information
    -i          | --file or folder : The log file path or log folder path
    -o          | --output folder  : The result output path, if not specified, outputs the result to the current folder
    --startTime | --start time     : Filter start time
    --endTime   | --end time       : Filter end time
    --user      | --user name      : Split by username
    --dbname    | --database name  : Split by database name
    --duration  | --duration       : Split by duration

### Examples:

    # Split log file "postgresql-2022-05-16_012020.csv" by database name
    PostgreSQLTweezer -i postgresql-2022-05-16_012020.csv --dbname

    # Split all log files in the folder "postgresql_log" by database name
    PostgreSQLTweezer -i postgresql_log --dbname

    # Split all log files in the folder "postgresql_log" by database name and user name
    PostgreSQLTweezer -i postgresql_log --dbname --user

    # Split all log files in the folder "postgresql_log" by database name and user name and duration
    PostgreSQLTweezer -i postgresql_log --dbname --duration --user

    # After filtering by start time and end time, split all log files in the folder "postgresql_log" by database name
    PostgreSQLTweezer -i postgresql_log --dbname  --startTime 2022-05-16_07:00 --endTime 2022-05-16_08:00
    

### AUTHORS
PostgreSQLTweezer is an original work from Robert Lee.

This web site is a work of Robert Lee.

PostgreSQLTweezer is maintained by Robert Lee and every one who wants to contribute.

Many people have contributed to PostgreSQLTweezer, they are all quoted in the Changelog file.

### LICENSE

PostgreSQLTweezer is free software distributed under the [Apache License, version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright (c) 2022-2022, Robert Lee

### mac compile command

    # Compiles to a Windows executable file
    CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -tags=prd -o cmd/win/PostgreSQLTweezer main/main.go
  
    # Compiles to a MacOS executable file
    go build -tags=prd -o cmd/mac/PostgreSQLTweezer main/main.go
  
    # Compiles to a Linux executable file
    GOOS=linux GOARCH=amd64 go build -tags=prd -o cmd/linux/PostgreSQLTweezer main/main.go
  

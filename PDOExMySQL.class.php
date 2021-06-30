<?php
    class PDOExMySQL
    {
        public $pdo;
        protected $dsn, $user, $password, $options;
        public $time_delta = NULL;
        private $profiling = FALSE;
        public $profiling_rowset_nums, $unprofiled_sqls, $unprof_sqls_times, $unprof_sqls_errors, $profiles;
        protected $profiling_switcher = 0; // -1: init disable; -2: done disable; 1: enable, 0: current mode
        protected $last_PDOStatement = NULL;
        public $last_sqls_count, $last_query_time = 0; // request server query and response time
        //private $try_max = 20;
        protected $profiling_max_history = '100';
        public $force_profiles = FALSE; // force request profiles every query
        public $unprof_executed_count = 0;
        
        function __construct($dsn, $user = NULL, $password = NULL, $options = NULL)
        {
            $this->dsn = $dsn;
            $this->user = $user;
            $this->password = $password;
            $this->options = [PDO::MYSQL_ATTR_MULTI_STATEMENTS => TRUE, PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => FALSE, PDO::ATTR_PERSISTENT => FALSE] + ($options ?? []); // надо PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => FALSE, иначе второй query не будет возвращать корректно результаты в rowsets начиная со второго
            $this->unprofiled_sqls = $this->unprof_sqls_times = $this->unprof_sqls_errors = $this->profiling_rowset_nums = []; // массивы вроде как не инициализируются в декларации
            // сделать проверку на mysql: в начале строки, если другой драйвер
            $this->pdo = new PDO($dsn, $user, $password, $this->options);
        }

        function reconnect()
        {
            $this->pdo = NULL;
            try
            {
                $this->pdo = new PDO($this->dsn, $this->user, $this->password, $this->options);
            }
            catch (PDOException $e)
            {
                return FALSE;
            }
            if ($this->profiling)
            {
                $this->profiling_switcher = 1;
                $this->profiling_rowset_nums = $this->unprofiled_sqls = $this->unprof_sqls_times = $this->unprof_sqls_errors = [];
                return $this->pdo->setAttribute(PDO::ATTR_STATEMENT_CLASS, ['PDOExMySQLStatement', [$this]]);
            }
            return TRUE;
        }

        function __call($method, $arguments)
        {
            return call_user_func_array([ $this->pdo, $method ], $arguments);
        }

        function adjust_time_delta($delta)
        {
            if ($this->time_delta !== NULL)
                $this->time_delta = ($this->time_delta + $delta) / 2;
            else
                $this->time_delta = $delta;
        }

        function server_time(float $local_time = NULL)
        {
            return ($local_time ?? microtime(TRUE)) + $this->time_delta ?? 0; // возвращает время на сервере, с учетой возможной разницы хода секунд    
        }

        function calibrate_time_delta($max_calib_time = 1, $max_count = 1)
        {
            if ($this->profiling)
                trigger_error('');

            $this->time_delta = NULL;
            $end = microtime(TRUE) + $max_calib_time;
            $i = 0;
            do
            {
                list($usec, $sec) = explode(' ', microtime());
                $ts = $sec.substr($usec, 1);
                $this->adjust_time_delta($this->query('SELECT UNIX_TIMESTAMP(NOW(6))-'.$ts)->fetch(PDO::FETCH_NUM)[0]);
                $i ++;
                if ($ts > $end)
                    break;
            }
            while ($i < $max_count);
            return $i;
        }        

        function enable_profiling($force = FALSE)
        {
            if ($this->profiling)
                return TRUE;
                        
            $this->profiling = TRUE;
            $this->force_profiling = $force;
            $this->profiling_switcher = 1;            
            return $this->pdo->setAttribute(PDO::ATTR_STATEMENT_CLASS, ['PDOExMySQLStatement', [$this]]);
        }

        function disable_profiling()
        {
            if (!$this->profiling)
                return FALSE;

            $this->profiling_switcher = -1;
            //$this->unprofiled_sqls = $this->unprof_sqls_times = $this->unprof_sqls_errors = [];
        }

        function set_profiling_max_history(int $value)
        { // если установить в 1, то результаты профайлинга будут доступны сразу, но сразу их можно получить также, если установить force_profiles в TRUE
            if ($this->profiling)
                trigger_error('PDOExMySQL change profiling max history after enable is bad idea', E_USER_WARNING);
            if ($value > 100 || $value < 1)
            {
                trigger_error('MySQL profiling max history is from 1 to 100. set_profiling_max_history param value is '.$value, E_USER_WARNING);
                $this->profiling_max_history = '100';
            }
            else
                $this->profiling_max_history = (string) $value;
        }

        private function check_last_PDOStatement() // нужно чтоб пройтись по всем rowset и те которые show profiles - выбрать профили
        {
            if ($this->profiling_rowset_nums != [] && $this->last_PDOStatement && $this->last_PDOStatement->currRowset < $this->last_sqls_count)
                $this->last_PDOStatement->closeCursor();            
        }

        function fetch_profiles()
        {
            $this->check_last_PDOStatement();

            if ($this->unprofiled_sqls != [])
            {
                $this->profiling_rowset_nums = [TRUE, FALSE];
                $this->pdo->query('SHOW PROFILES;SET profiling_history_size=0,profiling_history_size='.$this->profiling_max_history)->closeCursor();
                $this->last_sqls_count = 0;
                $this->last_PDOStatement = NULL;
            }

            $profiles = $this->profiles;
            $this->profiles = [];
            return $profiles;
        }
        
        protected function time_exec($sqls)
        {
            $t = microtime(TRUE);
            $cnt = $this->pdo->query($sqls);
            $this->last_query_time = microtime(TRUE) - $t;
            return $cnt;
        }

        protected function time_query($sqls, $fetch_style = NULL, $arg3 = NULL, $arg4 = NULL)
        {
            $t = microtime(TRUE);
            switch ($fetch_style)
            {
                case NULL:
                    $this->last_PDOStatement = $this->pdo->query($sqls);
                    break;
                case PDO::FETCH_COLUMN:
                case PDO::FETCH_INTO:
                    $this->last_PDOStatement = $this->pdo->query($sqls, $fetch_style, $arg3);
                    break;
                case PDO::FETCH_CLASS:
                default:
                    $this->last_PDOStatement = $this->pdo->query($sqls, $fetch_style, $arg3, $arg4);
            }
            $this->last_query_time = microtime(TRUE) - $t;
            return $this->last_PDOStatement;

            // return $this->last_PDOStatement = call_user_func_array([ $this->pdo, 'query'], [$sqls, $fetch_style, $arg3, $arg4]); // возвращается ошибка в связи с тем, что параметр 2 должен быть int            
        }

        protected function profiling_injection(&$i)
        {            
            $this->profiling_rowset_nums[$i] = TRUE;
            $this->profiling_rowset_nums[$i + 1] = FALSE;
            $this->last_sqls_count += 2;
            $i += 2;
            return 'SHOW PROFILES;SET profiling_history_size=0,profiling_history_size='.$this->profiling_max_history;
        }

        protected function profiling_query($sqls, $fetch_style = NULL, $arg3 = NULL, $arg4 = NULL)
        {            
            if ($this->profiling_switcher == -2)
            {
                $this->profiling = FALSE;
                $this->profiling_switcher = 0;
                $this->pdo->setAttribute(PDO::ATTR_STATEMENT_CLASS, ['PDOStatement', [$this->pdo]]);
                return $this->time_query(is_array($sqls) ? implode(';', $sqls) : $sqls, $fetch_style, $arg3, $arg4);
            }

            $this->unprof_executed_count = count($this->unprofiled_sqls);

            $this->check_last_PDOStatement();

            if ($this->profiling_switcher == -1)
            {                
                $this->profiling_switcher = -2;                    
                $multi_sql = 'SET profiling_history_size=0,profiling=0;'.(is_array($sqls) ? implode(';', $sqls) : $sqls);
                if ($this->unprofiled_sqls != [])
                {
                    $this->last_sqls_count = 1;
                    $this->profiling_rowset_nums = [TRUE, FALSE];
                    $multi_sql = 'SHOW PROFILES;'.$multi_sql;
                }
                else
                {
                    $this->profiling_rowset_nums = [FALSE];
                    $this->last_sqls_count = 0;
                }
                $sqls_count = 0;
            }
            else
            {
                if (!is_array($sqls))            
                    $sqls = split_sql($sqls);

                //var_dump($sqls);

                if ($this->profiling_switcher == 1)
                {
                    $multi_sql = 'SET profiling_history_size=0,profiling_history_size='.$this->profiling_max_history.',profiling=1';
                    $this->profiling_switcher = 0;
                    $i = 1;
                    $this->profiling_rowset_nums = [FALSE];
                }
                else
                {                
                    $i = 0;
                    $this->profiling_rowset_nums = [];

                    if ($this->unprof_executed_count >= $this->profiling_max_history)
                    {
                        if ($this->unprof_executed_count > $this->profiling_max_history)
                            trigger_error('PDOExMySQL unprofiled sqls '.$this->unprof_executed_count.' more than current max history: '.$this->profiling_max_history, E_CORE_WARNING);
        
                        $multi_sql = $this->profiling_injection($i);
                    }
                    else
                        $multi_sql = '';
                }

                $sqls_count = $this->last_sqls_count = count($sqls);

                $j = 0;
                foreach ($sqls as $sql)
                {
                    $j ++;
                    $this->unprofiled_sqls[] = $sql;
                    if ($multi_sql)
                        $multi_sql .= ';';
                    $multi_sql .= $sql;
                    $i ++;
                    if (count($this->unprofiled_sqls) % $this->profiling_max_history == 0 || ($this->force_profiles && $j == $sqls_count))
                        $multi_sql .= ';'.$this->profiling_injection($i);
                }
            }

                //echo "profiling_sql:\n".implode('; ', $sqls)."\n";
                //echo $multi_sql."\n";
                //echo "profiling_sql(rowset_nums):"; print_r($this->profiling_rowset_nums);
                //exit;
                        
            $qr = $this->time_query($multi_sql, $fetch_style, $arg3, $arg4);
            $this->unprof_sqls_times = array_replace($this->unprof_sqls_times, array_fill(array_key_first($this->unprofiled_sqls) ?? 0, $sqls_count, $this->last_query_time)); // нужно именно replace, т.к. через "+" добавляются старые индексы в конец, что не подходит для последующего slice
            //echo 'query (unprof_sqls_times):'; print_r($this->unprof_sqls_times);
            if (!$qr)
            {
                $errorInfo = $this->pdo->errorInfo();
                if ($errorInfo[1])                
                    PDOExMySQL_Handle_Error($this, $errorInfo);
            }
            //echo 'query (unprof_sqls_times):'; print_r($this->unprof_sqls_times);
            //echo 'query (unprofiled_sqls):'; print_r($this->unprofiled_sqls);
            //print_r($this->pdo->errorInfo());
            //var_dump($qr);
            return $qr;
        }

        function exec($sqls)
        {
            if ($this->profiling)
            {
                $qr = $this->profiling_query($sqls);
                $cnt = $qr->rowCount();
                $qr->closeCursor();
            }
            else
            {
                if (is_array($sqls))
                    $sqls = implode(';', $sqls);
                $t = microtime(TRUE);
                $cnt = $this->pdo->exec($sqls);
                $this->last_query_time = microtime(TRUE) - $t;
            }
            return $cnt;
        }

        function query($sqls, $fetch_style = NULL, $arg3 = NULL, $arg4 = NULL)
        {
            if (!$this->profiling)
                return $this->time_query(is_array($sqls) ? implode(';', $sqls) : $sqls, $fetch_style, $arg3, $arg4);

            return $this->profiling_query($sqls, $fetch_style, $arg3, $arg4);
        }
    }

    function PDOExMySQL_Handle_Error($pdoex, $errorInfo)
    {
        //echo 'check_error(errorInfo): '; print_r($errorInfo);
        //echo 'check_error(unprof_executed_count): '; var_dump($pdoex->unprof_executed_count);
        //echo 'check_error(unprofiled_sqls before slice): '; print_r($pdoex->unprofiled_sqls);

        $pdoex->unprof_sqls_errors[array_key_first($pdoex->unprofiled_sqls) + $pdoex->unprof_executed_count] = $errorInfo;
        $pdoex->unprof_executed_count ++;
        $pdoex->unprofiled_sqls = array_slice($pdoex->unprofiled_sqls, 0, $pdoex->unprof_executed_count, TRUE);        
        $pdoex->unprof_sqls_times = array_slice($pdoex->unprof_sqls_times, 0, $pdoex->unprof_executed_count, TRUE);

        //echo 'check_error(unprofiled_sqls after slice): '; print_r($pdoex->unprofiled_sqls);
        //echo 'check_error(unprof_sqls_errors): '; print_r($pdoex->unprof_sqls_errors);
        //echo 'check_error(unprof_sqls_times): '; print_r($pdoex->unprof_sqls_times);
    }

    class PDOExMySQLStatement extends PDOStatement
    {
        protected $pdoex;
        //protected $is_first_fetch = TRUE;
        public $currRowset = 0;
        protected $was_error = FALSE;

        protected function __construct($pdoex)
        {            
            $this->pdoex = $pdoex;            
            $this->check_rowset();
        }

        protected function check_error()
        {
            $errorInfo = $this->errorInfo();
            if ($errorInfo[1]) // после исчерпания nextRowset (=FALSE) в errorInfo[0] будет '00000', хотя в errorInfo[1] и errorInfo[2] ошибки
            {
                PDOExMySQL_Handle_Error($this->pdoex, $errorInfo);
                $this->pdoex->last_sqls_count = $this->currRowset;

                $this->was_error = TRUE;
                
                return FALSE;
            }
            return TRUE;
        }

        protected function check_rowset()
        {
            while (isset($this->pdoex->profiling_rowset_nums[$this->currRowset]))
            {
                if ($this->pdoex->profiling_rowset_nums[$this->currRowset])
                {
                    //echo 'check_rowset (unprofiled_sqls BEFORE fetch profiles):'; print_r($this->pdoex->unprofiled_sqls);
                    while ($profile = parent::fetch(PDO::FETCH_ASSOC))
                    {
                        //var_dump($profile);                        
                        //print_r($this->pdoex->unprofiled_sqls);
                        foreach ($this->pdoex->unprofiled_sqls as $i => $sql)
                         {// по идее без ошибок оно должно правильно работать и без проверки профиля
                            $profile_sql = split_sql($profile['Query'], FALSE)[0];
                            //var_dump($profile_sql);
                            //var_dump($sql);                            
                            
                            if (!strncmp($profile_sql, $sql, strlen($profile_sql)))
                            {
                                $profile += [
                                    'sql' => $sql,
                                    'full_time' => $this->pdoex->unprof_sqls_times[$i],
                                    'array_id' => $i // временно внутренняя переменная
                                ];
                                if (isset($this->pdoex->unprof_sqls_errors[$i]))
                                    $profile['error'] = $this->pdoex->unprof_sqls_errors[$i];
                                $this->pdoex->profiles[] = $profile;
                                $this->pdoex->unprof_executed_count --;
                                //echo 'unset before (unprof_sqls_times): '; print_r($this->pdoex->unprof_sqls_times);
                                unset($this->pdoex->unprofiled_sqls[$i], $this->pdoex->unprof_sqls_times[$i], $this->pdoex->unprof_sqls_errors[$i]); // пока тестирование не будем удалять неучтенные sql
                                //echo 'unset after (unprof_sqls_times): '; print_r($this->pdoex->unprof_sqls_times);
                                break;
                            }
                            else
                                trigger_error('Warning!!! Unprofiled SQL and profiled Query did not match: '.print_r($profile, TRUE).print_r($this->pdoex->unprofiled_sqls), E_USER_WARNING);

                            //unset($this->pdoex->unprofiled_sqls[$i], $this->pdoex->unprof_sqls_times[$i], $this->pdoex->unprof_sqls_errors[$i]);
                        }
                        //echo 'check_rowset(unprofiled_sqls): '; print_r($this->pdoex->unprofiled_sqls);   
                    }
                    //echo 'check_rowset (unprofiled_sqls AFTER fetch profiles):'; print_r($this->pdoex->unprofiled_sqls);
                }                

                unset($this->pdoex->profiling_rowset_nums[$this->currRowset]);

                //echo 'profiling_rowset_nums:';
                //print_r($this->pdoex->profiling_rowset_nums);

                $this->currRowset ++;
                if (!parent::nextRowset())
                {
                    $this->check_error();
                    return FALSE;
                }
            }

            $this->pdoex->unprof_executed_count ++;

            return TRUE;
        }

        /*function fetch($mode = PDO::FETCH_BOTH, $cursorMode = PDO::FETCH_ORI_NEXT, $cursorOffset = 0)
        {
            if ($this->is_first_fetch && !$this->check_rowset())
                return FALSE;                
            $this->is_first_fetch = FALSE;            
            return parent::fetch($mode, $cursorMode, $cursorOffset);
        }

        function fetchAll($mode = PDO::FETCH_BOTH, $arg2 = NULL, $arg3 = NULL)
        {            
            if (!$this->check_rowset())
                return [];

            switch ($mode)
            {
                case PDO::FETCH_COLUMN:
                case PDO::FETCH_INTO:                    
                    return parent::fetchAll($mode, $arg2);
                case PDO::FETCH_CLASS:
                    return parent::fetchAll($mode, $arg2, $arg3);
                default:
                    return parent::fetchAll($mode);
            }
        }*/

        function nextRowset()
        {
            $this->currRowset ++; // если вызываем уже ложно nextRowset, то впринципе не важен этот инкремент
            //$this->is_first_fetch = TRUE;
            if (!parent::nextRowset())
            {
                if (!$this->was_error)
                    $this->check_error();
                return FALSE;
            }            

            return $this->check_rowset();
        }

        function closeCursor()
        {           
            while ($this->nextRowset()) {}
        }
    }

    function split_sql($sql, $rtrim = TRUE)
    {
        $re = '%
            \s*                                                 # Discard leading whitespace.
                ((?:                                            # Group for content alternatives.
                  \'[^\'\\\\]*(?:\\\\.[^\'\\\\]*)*\'?           # Either a single quoted string,
                | "[^"\\\\]*(?:\\\\.[^"\\\\]*)*"?               # or a double quoted string,
                | `[^`]*`?                                      # single back quoted string
                | \/\*(?:.*?\*\/|.*)                            # or a multi-line comment,
                | (?-s)\#.*                                     # or a # single line comment (off s modifier from here),                
                | [^;"\'#`\/-]+                                 # or one non-[...]
                | --.*                                          # or a -- single line comment,
                | -                                             # or -
                )*)                                             # One or more content alternatives
                ;                                               # Record end is a ; or string end.
            %xs';
        if ($rtrim)
            $sql = rtrim($sql, "; \t\n\r\0\x0B");
        if (preg_match_all($re, $sql.';', $matches))
            return $matches[1];        
        return [];
    }
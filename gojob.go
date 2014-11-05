/**
workerid,   服务id
addr,       监听地址
pwd,        认证密码
master,     控制端地址


createjob,  创建一个计划任务
deletejob,  删除一个计划任务
startjob,   启动一个计划任务
runjob,     立即执行一个计划任务
stopjob,    停止一个计划任务
killjob,    停止一个计划任务并杀死所有执行中的task任务
statjob,    查看指定jobid的状态

killtask,   杀死指定taskid的task任务
killalltask,  杀死所有task任务
stattask    查看指定taskid的任务状态
gettasklog  查看指定taskid的执行log

guard       守护执行进程，执行参数，执行数量，尝试次数按照非波那且延时，或者指定间隔时间，尝试启动，还是尝试杀死，还是尝试重启

stat        查看当前概况

 
 */
package main

import (
    "encoding/json"
    "log"
    "os"
    "time"
    "fmt"
    "strconv"
    "net/http"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
    "github.com/robfig/cron"
)

var DB *sql.DB
var c = cron.New()

type Job struct {
    jobid string
    spec string
    bin string
    args []string
}
var jobs  = make(map[string] Job)

func main() {
    initdb()
    Start()
}

/**
 * 初始化
 */
func initdb() {
    db, err := sql.Open("sqlite3", "./gojob.db")
    DB = db
    if err != nil {
        log.Fatal(err)
    }
    _, err = DB.Exec(`
        CREATE TABLE IF NOT EXISTS job(
            id INTEGER  PRIMARY KEY AUTOINCREMENT,
            jobid varchar(50) NOT NULL DEFAULT '' UNIQUE,
            spec varchar(50) NOT NULL DEFAULT '',
            bin varchar(1024) NOT NULL DEFAULT '',
            args varchar(1024) NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS task(
            id INTEGER  PRIMARY KEY AUTOINCREMENT,
            taskid varchar(50) NOT NULL DEFAULT ''  UNIQUE,
            jobid INTEGER NOT NULL DEFAULT 0,
            starttime INTEGER NOT NULL DEFAULT 0,
            exittime INTEGER NOT NULL DEFAULT 0,
            stat INTEGER  COMMENT '状态，默认0准备启动，1启动失败，2启动成功，3执行失败 4执行成功，5失去监控' NOT NULL DEFAULT 0,
            pid INTEGER NOT NULL DEFAULT 0,
            cmdline varchar(1024) NOT NULL DEFAULT ''

        );

        UPDATE task SET stat = 5 WHERE stat IN (0, 2);
    `)
    if err != nil {
        log.Fatal(err)
    }

    // 加载cron到内存
    rows, err := DB.Query(`
        SELECT jobid, spec, bin, args FROM job;
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    fmt.Println(rows)
    
    for rows.Next() {
        var jobid string
        var bin string
        var spec string
        var args_str  string
        rows.Scan(&jobid, &spec, &bin, &args_str)
        fmt.Println(jobid, spec, bin, args_str)
        
        var args []string
        json.Unmarshal([]byte(args_str), &args)
        jobs[jobid] = Job{jobid, spec, bin, args}

        fmt.Println("addjob, ", jobid)
        c.AddFunc(spec, func() {
            runjob(jobid)
        })
    }
    c.Start()
   
}

func runjob(jobid string) {
    fmt.Println(jobs[jobid])
    exec(jobid, jobs[jobid].bin, jobs[jobid].args)
}
/**
 * 启动服务
 */
func Start() {
    // 启动http服务
    http.HandleFunc("/", reqFunc)
    http.ListenAndServe(":3721", nil)
}

/**
 * http请求监听
 */
func reqFunc(w http.ResponseWriter, r *http.Request) {
    r.ParseForm()

    action := r.FormValue("action")
    switch action {
        case "setjob":
            spec := r.FormValue("spec")
            bin := r.FormValue("bin")
            args := r.Form["args[]"]
            jobid := setjob(spec, bin, args)
            w.Write([]byte(jobid))
            
        // 设置一次性延时job
        case "setjob_once_delay":
            /*
            delay, _ := strconv.ParseInt(r.FormValue("delay"), 10, 0)
            bin := r.FormValue("bin")
            arg := r.Form["arg[]"]
            jobid := setjob_once_delay(delay, bin, arg)
            w.Write([]byte(jobid))
            */
        case "del":
        case "get":
        case "stat":
        default:
            w.Write([]byte(`
            help
            `))

    }
}

func setjob(spec string, bin string, args []string) (string) {
    jobid := strconv.FormatInt(time.Now().UnixNano(), 10)
    sql := `
        INSERT INTO job(jobid, spec, bin, args) VALUES (?, ?, ?, ?);
    `
    fmt.Println(DB)
    stmt, err := DB.Prepare(sql)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(args)
    arg ,_:= json.Marshal(args)
    _, err = stmt.Exec(jobid, spec, bin , arg)
    if err != nil {
        log.Fatal(err)
    }

    jobs[jobid] = Job{jobid, spec, bin, args}
    c.AddFunc(spec, func() {
        runjob(jobid)
    })
    return jobid
}
func setjob_once_delay(delay int64, bin string, arg []string) (string) {
    jobid  := strconv.FormatInt(time.Now().UnixNano(), 10)
    return jobid
    //time.Sleep(time.Duration(delay) * time.Second)
    //exec(bin, arg)
}

func exec(jobid string, bin string, args []string) {

    taskid := strconv.FormatInt(time.Now().UnixNano(), 10)

    sql := `
        INSERT INTO task (taskid, jobid, starttime) VALUES(?, ?, ?);
    `
    smtm, err := DB.Prepare(sql)
    smtm.Exec(taskid, jobid, time.Now().Unix())

    logpath := "./"
    
    errlog, err := os.Create(logpath + taskid + "_err.log")
    outlog, err := os.Create(logpath + taskid + "_out.log")
    attr := &os.ProcAttr{
        Files: []*os.File{os.Stdin, outlog, errlog},
    }
    args = append([]string{bin}, args...)

    fmt.Println("start proce, ", bin, args)
    p, err := os.StartProcess(bin, args, attr)
    if err != nil {
        defer func() {
            fmt.Println("start err defer")
            smtm, _ = DB.Prepare(`
                UPDATE task set stat = 1 
                WHERE taskid = ?
            `)
            smtm.Exec(taskid)
        }()
        return
    }
    fmt.Println("pid: " , p.Pid)

    smtm, _ = DB.Prepare(`
        UPDATE task SET stat = 2, pid = ?
        WHERE taskid = ?
    `)
    smtm.Exec(p.Pid, taskid)

    ps, err := p.Wait()
    fmt.Println("pid exit:", ps.Pid(), ps.Success(), ps.String())
    
    sql = `
        UPDATE task SET exittime = ?, stat = ?
        WHERE taskid = ?
    `
    smtm, err = DB.Prepare(sql)
    stat := 3 
    if ps.Success() {
        stat = 4
    }
    smtm.Exec(time.Now().Unix(), stat,  taskid)
}

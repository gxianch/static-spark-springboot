package com.zhuinden.sparkexperiment;

import com.rczl.entity.Play;
import com.rczl.service.LauchHotelStatic;
import com.rczl.service.LauchStatic;
import com.rczl.service.PlayStatic;
import com.rczl.service.PlayTrackStatic;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by Owner on 2017. 03. 29..
 */
@RequestMapping("api")
@Controller
public class ApiController {
    @Autowired
    WordCount wordCount;
    @Autowired
    private SparkSession sparkSession;
    @Value("${filepath}")
    private String filepath;
    private static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");//设置日期格式
    @RequestMapping("wordcount")
    public ResponseEntity<List<Count>> words() {
        return new ResponseEntity<>(wordCount.count(), HttpStatus.OK);
    }
    //播放次数排行榜,暂编码今日 TODO
    @RequestMapping("static/play/rank/{day}")
    public ResponseEntity<String> rank(@PathVariable String day) {
        File log = null;
        String path = null;
        if("today".equals(day)){
            Calendar cal = Calendar.getInstance();
            File file = new File(filepath + File.separator + cal.get(Calendar.YEAR)
                    + File.separator + (cal.get(Calendar.MONTH)+1) );  //测试
            path = file+ File.separator +yyyyMMdd.format(new Date())+"play-vod.log";
            log  =   new File(file+ File.separator +yyyyMMdd.format(new Date())+"play-vod.log");
            if(!log.exists()) {//文件不存在则直接退出
                return null;
            }
        }
        PlayStatic playStatic =   new PlayStatic(sparkSession,path);
        return new ResponseEntity<String>(playStatic.rank().toString(), HttpStatus.OK);
    }
    //追踪媒资今日播放记录
    @RequestMapping("static/play/track/{mediaId}")
    public ResponseEntity<String> trackMedia(@PathVariable String mediaId) {
        Calendar cal = Calendar.getInstance();
        File file = new File(filepath + File.separator + cal.get(Calendar.YEAR)
                + File.separator + (cal.get(Calendar.MONTH)+1) );  //测试
        String  path = file+ File.separator +yyyyMMdd.format(new Date())+"play-vod.log";
        File log  =   new File(file+ File.separator +yyyyMMdd.format(new Date())+"play-vod.log");
        if(!log.exists()) {//文件不存在则直接退出
            return null;
        }
        PlayTrackStatic playTrackStatic =  new PlayTrackStatic(sparkSession,path,mediaId);
        playTrackStatic.track();
        return   new ResponseEntity<String>(HttpStatus.OK);
    }
    //机顶盒月开机数
    @RequestMapping("static/launch/mac/{date}")
    public  ResponseEntity<String> launch(@PathVariable String date) {
        LauchStatic lauchStatic =   new LauchStatic(sparkSession,filepath,date);
        return  new ResponseEntity<>(lauchStatic.apply(), HttpStatus.OK);
    }
    //酒店平均开机率
    @RequestMapping("static/launch/hotel/{date}")
    public  ResponseEntity<String> hotelrate(@PathVariable String date) {
        LauchHotelStatic lauchHotelStatic =   new LauchHotelStatic(sparkSession,filepath,date);
        return  new ResponseEntity<>(lauchHotelStatic.apply(), HttpStatus.OK);
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.library.scheduler.executortask;

import com.library.utilities.LoggerUtil;

/**
 *
 * @author smallgod
 */
public class ScheduleAdTask implements Runnable{
    
   private static final LoggerUtil LOGGER = new LoggerUtil(ScheduleAdTask.class);

    @Override
    public void run() {
    
        LOGGER.debug("Task will run here..");
    }
    
}

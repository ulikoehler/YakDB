/* 
 * File:   JobController.hpp
 * Author: uli
 *
 * Created on 13. Juli 2013, 15:32
 */

#ifndef JOBCONTROLLER_HPP
#define	JOBCONTROLLER_HPP
#include <thread>
#include <vector>
#include "AbstractFrameProcessor.hpp"

class JobController : private AbstractFrameProcessor {
public:
    JobController();
    ~JobController();
    void waitForAll();
private:
    TableOpenHelper tableOpenHelper;
    Tablespace& tablespace;
    std::vector<std::thread*> threads;
};

#endif	/* JOBCONTROLLER_HPP */


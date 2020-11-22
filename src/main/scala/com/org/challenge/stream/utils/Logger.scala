package com.org.challenge.stream.utils

trait Logger {
    @transient lazy val log = org.apache.log4j.LogManager.getLogger("appLogger")
}

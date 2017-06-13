package com.datastax.events.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.joda.time.DateTime;

public class DateUtils {
	private static final SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyyMMdd-hhmmss");

	static public DateTime parseDate(String date) throws ParseException{
		
		try {
			return new DateTime(inputDateFormat.parse(date));
		} catch (ParseException e) {
			throw e;
		}
	}
}

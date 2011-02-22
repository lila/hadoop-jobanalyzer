#!/usr/bin/env groovy

import java.util.HashMap;
import java.net.*;
import java.io.*;
import java.util.regex.*
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import com.oreilly.servlet.multipart.MultipartParser;

String gridauthPropertiesFile = "hadoop-jobanalyzer.properties"

props = new Properties()
props.load(Logger.getClassLoader().getResourceAsStream(gridauthPropertiesFile))
PropertyConfigurator.configure(props)

Logger log = Logger.getLogger("hadoop-jobanalyzer")


HashMap<String, String> decodeQueryString(String qs) throws UnsupportedEncodingException
{
    if (!qs) return;
    qs = qs.trim();
    String[] pairs = qs.split("&");
    HashMap vars = new HashMap<String, String>();

    if (qs.equals("")) return vars;

    for (int i=0; i<pairs.length; i++) {
	String[] fields = pairs[i].split("=");
	String name = URLDecoder.decode(fields[0], "UTF-8");
	String value = URLDecoder.decode(fields[1], "UTF-8");
	vars.put(name, value);
    }

    return vars;
}

HashMap<String, String> parsePostDataUrlEncodedMultiPart(String boundary) throws UnsupportedEncodingException
{
    int length = System.in.available();
    HashMap vars = new HashMap<String, String>();

    byte[] buffer = new byte[length];
    System.in.read(buffer);

    String b = new String(buffer)
	println("buffer=${b}")

    b.split(boundary).each { line ->
       m = line =~ '(?s)name="(.*)"[\r\n]*(.*)[\r\n]*$'
       	if (m) {
	    vars[m[0][1]] = m[0][2].trim();
    	}

    }

    return vars;
}


HashMap<String, String> parsePostDataUrlEncoded() throws UnsupportedEncodingException
{
    int length = System.in.available();
    HashMap vars = new HashMap<String, String>();

    byte[] buffer = new byte[length];
    System.in.read(buffer);

    String b = new String(buffer)
	println("buffer=${b}")

    b.split('&').each { line ->
       m = line =~ '(?s)(.*)=(.*)$'
       if (m) {
	    vars[m[0][1]] = m[0][2].trim();
		}

    }

    return vars;
}


print "Content-type: text/plain\n\n"

print "ENVIRNONMENT VARS\n"

System.env.each {k, v -> println("${k} = ${v}") }

HashMap<String, String> env;
HashMap<String, String> get;
HashMap<String, String> post;
HashMap<String, String> cookies;
    
  /*
   * get all the parameters.
   *
   * parameters are sent via post, but some existing gridauth clients use multi-part
   * form data for all their data values.  this is due in part to how libcurl works with
   * php, and standard servelets don't support multi-part forms.  but for the sake of
   * backward compatibility, this script supports both wwww-urlencoded and multi-part
   */
  post = [:]

  try {
    def multi = new MultipartParser(request, 1024 * 100)

    while ((part = multi.readNextPart()) != null) {
      if (part.isParam()) {
        post[part.getName()] = part.getStringValue()
        log.debug("post = ${post}")
      }
    }

  } catch (Exception e) {
  }

  p = request.getParameterMap()
  p.each {k, v ->  post[k] = v[0]}

  print("parameters are:");
  post.each {k, v -> print("${k}/${v}") }

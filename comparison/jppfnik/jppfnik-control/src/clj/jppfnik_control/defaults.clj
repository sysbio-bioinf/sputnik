; Copyright (c) Gunnar Völkel. All rights reserved.
; The use and distribution terms for this software are covered by the
; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; which can be found in the file epl-v1.0.txt at the root of this distribution.
; By using this software in any fashion, you are agreeing to be bound by
; the terms of this license.
; You must not remove this notice, or any other, from this software.

(ns jppfnik-control.defaults)


(def server-defaults
{
	:jppf
	{
    ; port number on which the server listens for connections
    :server.port 11111	    
    
    ; Each JPPF driver is able to run a single node in its own JVM, called ”local node”.
    ; The main advantage is that the communication between server and node is much faster, since the network overhead is removed.
    :local.node.enabled false

    :peer
    {
      ; Enable/disable auto-discovery for peer-to-peer communication between drivers
      :discovery.enabled false
      ; toggle to enable secure connections to remote peer servers, defaults to false
      :ssl.enabled false
    }
    
    ; Example for ssl configuration
;    :ssl
;    {
;      :server.port 11443
;      ; specify the location of the SSL configuration as a file, 
;      ; e.g. :ssl.configuration.file congig/ssl.properties
;      :configuration.file nil
;    }    
    
    ; Other JVM options added to the java command line when the server is started
    ; as a subprocess. Multiple options are separated by spaces
    :jvm.options "-server"
    ;:jvm.options "-server -Xmx256m"
    
	  :management
	  {
		  ; Enabling JMX features
		  :enabled true
		  ; JMX management host IP address
		  :host "localhost"
		  ; JMX management port
		  :port 11198
    
      :ssl
      {
        ; Enabling JMX features via secure connections, defaults to false
        :enabled false
        ; secure JMX server port
        :port nil
      }
	  }
  
    :discovery
    {
	    ; Enable/Disable automatic discovery of JPPF drivers
	    :enabled false ; we do not want discovery by default
	    ; UDP multicast group to which drivers broadcast their connection parameters
	    :group "230.0.0.1"
	    ; UDP multicast port to which drivers broadcast their connection parameters
	    :port 11111
	    ; How long a driver should wait between 2 broadcasts, in milliseconds
	    :broadcast.interval 1000
    }
    
    :load.balancing
    {
      ; Load-balancing algorithm
      :algorithm "nodethreads"
      ; parameters profile name
      :strategy "nodethreads_profile"
    }
    
    :recovery
    {
      ; Enable recovery from hardware failures on the nodes.
      ; Default value is false (disabled).
      :enabled false
      ; Maximum number of attempts to get a response form the node before the
      ; connection is considered broken. Default value is 3.
      :max.retries 3
      ; Maximum time in milliseconds allowed for each attempt to get a response
      ; from the node. Default value is 6000 (6 seconds).
      :read.timeout 6000
      ; Dedicated port number for the detection of node failure.
      ; Default value is 22222.
      :server.port 22222
      
      :reaper
      {
				; Interval in milliseconds between two runs of the connection reaper.
				; Default value is 60000 (1 minute).
        :run.interval 60000
        ; Number of threads allocated to the reaper.
        ; Default value is the number of available CPUs.     
        :pool.size nil
      }
    }
  }

  ; The JPPF driver uses 2 pools of threads to perform network I/O with the nodes in parallel. 
  ; One pool is dedicated to sending and receiving job data, the other is dedicated to the 
  ; distributed class loader. There is a single configuration property that specifies the size 
  ; of each of these pools:
  :transition.thread.pool.size nil

  :strategy
  {
    ; "manual" profile
    :manual_profile.size 1
    
    ; "autotuned" profile
    :autotuned_profile
    {
      :size 5
      :minSamplesToAnalyse 100
      :minSamplesToCheckConvergence 50
      :maxDeviation 0.2
      :maxGuessToStable 50
      :sizeRatioDeviation 1.5
      :decreaseRatio 0.2
    }
    
    ; "proportional" profile
    :proportional_profile
    {
      :size 5
      :performanceCacheSize 3000
      :proportionalityFactor 2
    }
    
    ; "rl" profile
    :rl_profile
    {
      :performanceCacheSize 3000
      :performanceVariationThreshold 0.001
    }
    
    ; "nodethreads" profile
    ; means that multiplicator * nbThreads tasks will be sent to each node
    :nodethreads_profile.multiplicator 2
  }
})


(def node-defaults
{
  :jppf
  {
    :server
    {
      ; Host name, or ip address, of the host the JPPF driver is running on
      :host "localhost"
      ; JPPF server port number
      :port 11111
    }
    
    :management
    {
      ; Enabling JMX features
      :enabled true
      ; JMX management host IP address
      :host "localhost"
      ; JMX management port
      :port 11198      
      
      :ssl
      {
        ; Enabling JMX features via secure connections, defaults to false
        :enabled false
        ; secure JMX server port
        :port nil
      }  
    }    
    
    :discovery
    {
      ; Enable/Disable automatic discovery of JPPF drivers
      :enabled false ; we do not want discovery by default
      ; UDP multicast group to which drivers broadcast their connection parameters
      :group "230.0.0.1"
      ; UDP multicast port to which drivers broadcast their connection parameters
      :port 11111
      ; How long the  node will attempt to automatically discover a driver before
      ; falling back to the parameters specified in this configuration file
      :timeout 5000
      ; IPv4 address inclusion patterns
		  :include.ipv4 nil 
		  ; IPv4 address exclusion patterns
		  :exclude.ipv4 nil 
		  ; IPv6 address inclusion patterns
		  :include.ipv6 nil		 
		  ; IPv6 address exclusion patterns
		  :exclude.ipv6 nil 
    }
    
    :recovery
    {
      ; Enable recovery from hardware failures on the node.
      ; Default value is false (disabled).
      :enabled false
			; Dedicated port number for the detection of node failure, must be the same as
			; the value specified in the server configuration. Default value is 22222.
      :server.port 22222
			; Maximum number of attempts to get a message from the server before the
			; connection is considered broken. Default value is 2.
      :max.retries 2
			; Maximum time in milliseconds allowed for each attempt to get a message
			; from the server. Default value is 60000 (1 minute).
      :read.timeout 60000
    }    
    
    ; path to the JPPF security policy file
    :policy.file nil ; "config/jppf.policy"
    
    ; Other JVM options added to the java command line when the node is started as
    ; a subprocess. Multiple options are separated by spaces
    :jvm.options "-server"
    ;:jvm.options "-server -Xmx256m"
    
    ; size of the node's class loader cache
    :classloader.cache.size 50
    
    :ssl
    {
      ; Enable SSL. Default value is false (disabled)
      ; If enabled, only SSL/TLS connections are established
      :enabled false
      
      ; specify the location of the SSL configuration as a file, 
      ; e.g. :ssl.configuration.file congig/ssl.properties
      :configuration.file nil
    }
  }
  
  ; Processing Threads: number of threads running tasks in this node
  :processing.threads nil
  
  :reconnect
  {
    ; Automatic recovery: number of seconds before the first reconnection attempt
    :initial.delay 1
    ; Time after which the system stops trying to reconnect, in seconds
    :max.time 60
    ; Automatic recovery: time between two connection attempts, in seconds
    :interval 1
  }
})


(def client-defaults
{
  :jppf
  { 
    ; list of drivers this client may connect to
    :drivers "default-driver"
    
    ; connection pool size for each discovered server; defaults to 1 (single connection)
    :pool.size 1
    
    :ssl
    {
      ; Enable SSL. Default value is false (disabled)
      ; If enabled, only SSL/TLS connections are established
      :enabled false
      
      ; specify the location of the SSL configuration as a file, 
      ; e.g. :ssl.configuration.file congig/ssl.properties
      :configuration.file nil
    }
    
    :discovery
    {
      ; enable/disable automatic discovery of JPPF drivers
      :enabled false ; we do not want discovery by default
      ; UDP multicast group to which drivers broadcast their connection parameters
      :group "230.0.0.1"
      ; UDP multicast port to which drivers broadcast their connection parameters
      :port 11111
      ; IPv4 address inclusion patterns
		  :include.ipv4 nil 
		  ; IPv4 address exclusion patterns
		  :exclude.ipv4 nil 
		  ; IPv6 address inclusion patterns
		  :include.ipv6 nil		 
		  ; IPv6 address exclusion patterns
		  :exclude.ipv6 nil 
    }
    
    ; It is possible for a client to execute jobs locally (i.e. in the client JVM) rather than 
    ; by submitting them to a server. This feature allows taking advantage of muliple CPUs or 
    ; cores on the client machine, while using the exact same APIs as for a distributed remote execution.
    :local.execution
    {
      ;enable/disable local job executio
      :enabled false
      ; number of threads to use for local execution
      :threads nil
      
      ; You can specify how frequently you wish to receive notfiications of locally executed tasks, 
      ; using either or both of the following parameters.
      ; If both accumulation time and size are used at the same time, a notification will be sent 
      ; whenever the size is reached, or the time is reached, whichever happens first. 
      ; If neither is specified, the tasks wil be returned all at once. 
      :accumulation
      {
        ; specifies for how many completed tasks to wait before a notification is sent
        :size 4
				; specifies how long to wait before a notification is sent
				; (if any task has completed)
        :time 100
        ; specifies the time unit:
				;   n = nanoseconds        M = minutes
				;   m = milliseconds       h = hours
				;   s = seconds            d = days
        :unit "m"
      }
    }
    
    ; The JPPF client allows load balancing between local and remote execution. 
    ; The load balancing configuration is exactly the same as for the driver, 
    ; which means it uses exactly the same configuration properties, algorithms, 
    ; parameters, etc... Please refer to the driver load-balancing configuration section 
    ; for the configuration details. The default configuration, if none is provided, 
    ; is equivalent to the following: 
    :load.balancing
    {
      ; name of the load balancing algorithm
      :algorithm "manual"
      ; name of the set of parameter values (aka profile) to use for the algorithm
      :strategy "manual_profile"
    }
  }
  
  :reconnect
  {
    ; Automatic recovery: number of seconds before the first reconnection attempt
    :initial.delay 1
    ; Time after which the system stops trying to reconnect, in seconds
    :max.time 60
    ; Automatic recovery: time between two connection attempts, in seconds
    :interval 1
  }
  
  :strategy.manual_profile
  {
    :size Integer/MAX_VALUE  
  }  
  
  
  ; this defines the default driver, other drivers can be define likewise and have to be added to :jppf.drivers
  :default-driver
  {
    :jppf
    {
      :server
      {
        ; host name, or ip address, of the host the JPPF driver is running on
        :host "localhost"
        ; server port number
        :port 11111
      }
      
      :management
      {
        ; host name for the management server
        :host "localhost"
        ; port number for the management server
        :port 11198
      }
      
      ;connection pool size for this driver
      :pool.size 1
    }
    
    ; priority given to the driver connection
    :priority 0
  }
})


; HOWTO create keystore & truststore: http://www.techbrainwave.com/?p=953
(def ssl-defaults
{
  :jppf.ssl
  {
    ; SSLContext protocol, defaults to SSL
    ; A list of valid protocol names:
    ; SSL      Supports some version of SSL; may support other versions
    ; SSLv2    Supports SSL version 2 or higher; may support other versions
    ; SSLv3    Supports SSL version 3; may support other versions
    ; TLS      Supports some version of TLS; may support other versions
    ; TLSv1    Supports RFC 2246: TLS version 1.0 ; may support other versions
    ; TLSv1.1  Supports RFC 4346: TLS version 1.1 ; may support other versions
    ; from http://docs.oracle.com/javase/6/docs/technotes/guides/security/StandardNames.html#SSLContext
    :context.protocol "SSL"
  
    ; list of space-separated enabled protocols (available: SSLv3, TLSv1, SSLv2Hello)
    ; from http://docs.oracle.com/javase/6/docs/technotes/guides/security/SunProviders.html#SunJSSEProvider
    :protocols "SSLv2Hello SSLv3"
  
    ; enabled cipher suites as a list of space-separated values
    ; for available cipher suites see http://docs.oracle.com/javase/6/docs/technotes/guides/security/StandardNames.html#SupportedCipherSuites
    :cipher.suites "SSL_RSA_WITH_RC4_128_MD5 SSL_RSA_WITH_RC4_128_SHA"
  
    ; client authentication mode - possible values: none | want | need
    :client.auth "none"
  
	  :keystore
	  {
	    ; path to the keystore on the file system
	    ; e.g. :file "config/ssl/keystore.ks"
      :file nil
	   
      ; keystore password in clear text
	    ; e.g. :password "password"
	    :password nil
	  }
  
	  :truststore
	  {
	    ; path to the truststore on the file system
      ; e.g. :file "config/ssl/truststore.ks"
      :file nil
	   
      ; truststore password in clear text
	    ; e.g. :password "password"
	    :password nil
	  }
  }
})


(def default-config
  {
   :sputnik/server server-defaults,
   :sputnik/node   node-defaults,
   :sputnik/client client-defaults,
   :sputnik/ssl    ssl-defaults
  })

# --- header
require 'socket'
require_relative 'tix.rb'

# --- define class and methods
class Client
   def initialize(all_hosts, all_ports, all_pids, pid, log)
      
      # passed arguments
      @ackcount = 0
      @logcount = 0
      @h_b_no = 0
      
      @allhosts   = all_hosts
      @allports   = all_ports
      #puts "allports--------------- #{@allports[2]}"
      @allpids    = all_pids
      @allstates  = Array.new(@allports.length, false)
      @allconns   = [@allhosts, @allports, @allpids, @allstates] # just an array of pointers to all these arrays, right?
      @majority = 0
      @total = 0
      
      @allsocks   = Array.new(@allports.length)
      #@allsocks   = [[Array.new(@allports.length)], [@allstates]] 
      #    # populate allports with array of socket IDs (currently nil) and 
      #    # array of booleans indicating if the connection is live
      
      @pid        = pid
      
      @log        = log #array of strings, where msgs passed are which log entry is what
      
      # global connection variables
      @lport      = @allports[@pid]
      @lhost      = @allhosts[@pid]
      @logcommit  = []
      @msgq       = Array.new(@allpids.length) {[]} # array of string messages 
      @sendq      = [] # array of pids corresponding to messages needing to be sent
      
      # paxos variables
      @leaderpid       = 1         # pid of kiosk known to be leader
      @startelection   = false   #set this when you find out you have the highest live pid
      @lastbaln        = 0      # highest ballot number seen so far
      @lastbalpid      = 0 # assume 0, can't hurt if proc 0 starts something
      @lastval         = "default log msg"
      @ebal            = 0  #best default msg?
      @eval            = "empty ballot" 
      @etal            = 0  # assume no tallies to start
      
      # begin listening on own port
      @lsock = TCPServer.open(@lhost, @lport)
      logi = log.length #get current length, which coincidentally gives next open index
      updatelog("#{logi}:Kiosk #{@pid} is initialized")
      
      # begin opening ports to other kiosks
      Thread.start{connect}
          # connect to others *** need a method to reconnect (try reconnecting periodically) if one fails
          # try opening TCP to each other here, but also handle failures later on when others crash
          if @leaderpid == @pid
              Thread.start{heartbeat}
          end
       # begin listening for connections from other kiosks
      Thread.start{listen}
      
      # begin checking msg q for msgs to send
      Thread.start{sender}
      
      # do main action - user interface
      run
          # final routine to run
      
      
      # ### updatelog test
      # updatelog("0:First entry")
      # updatelog("3:4 tix sold by kiosk 3")
      # updatelog("2:9 tix sold by kiosk 1")
      
      # @log.each do |e| puts e end
   end # end init
   
   # bogus routine to never shut down while testing other methods
   def runbogus
    loop{
      sleep(2)
      puts "Running"
    }
   end
   
   def run
     # watch for user input to buy tickets or show state
     
           ### logic for client buying tickets
     loop { 
      puts "Enter 's' or number of tickets to purchase: \n"
      u = $stdin.gets.chomp # get user input
     
      if u == "s" # if show state command
        print "Log: \n"
        #@log.each do |e| print "#{e}\n" end
        logname = "log#{@pid}.log"
        f1 = File.open(logname,'r')
        f1.each_line do |line|
            puts line
        end
        f1.close
      
      else # if not a letter then u is a number of tickets to buy
        ntix = u.to_i
        print "Now buying #{ntix} tix ... please stand by\n"
        #launch paxos election here
        
        logi = @log.length
        
        proposedlog = "#{logi}:Kiosk #{@pid} sold #{ntix} tickets"
        #print "run - now trying msg: '#{proposedlog}\n"
      
        #put opid in sendq and msg in sendq
        
        # if I'm the leader
        if @leaderpid == @pid
            
         # check if there are enough tickets ------- tix
         print "---------------------------Available tickets are #{Tix::get_value}\n"
         if Tix::get_value - ntix < 0
             print "NOOOOOOOOO NOT ENOUGH TICKETS!\n"
             next
             
         else
           print "buying #{ntix} tickets!"
            Tix::reduce(ntix) 
            print "-------------------------Available tickets are #{Tix::get_value}\n"
         end
         # then send log accept to all participants and wait for acks before accepting
         
         # ###*** need to check log entry proposal too? which entry to make this?
         
         # Build paxos msg with "tellaccept"
          msg = "tellaccept,0,0,#{proposedlog},0,#{@pid}" #msg to known leader
        
          sendall(msg)
        
        # @allpids.each do |opid|
        #     if opid == @pid then next end #own pid
        #     @sendq << opid
        #     @msgq[opid] << msg 
        # end #allpids loop

        elsif @leaderpid != nil
        
          # Build paxos msg with "request"
          msg = "request,0,0,#{proposedlog},0,#{@pid}" #msg to known leader
          # if I'm not the leader and I have a leader, then send to leader
          
          @sendq << @leaderpid
          @msgq[@leaderpid] << msg 
          
          ### ***check if this succeeds - what if leader crashed?
       
        else #start an election
           
          print "run - kiosk #{@pid} starting an election\n"
          # # Build paxos msg with "prepare"
          baln = @lastbaln + 1
          bal = "#{baln}:#{@pid}" 
          msg = "prepare,#{bal},0,#{proposedlog},0,#{@pid}" #msg to known leader
                    #set (or reset) election global vars - if prev failed, these will be reset here.
          #if acks arrive after later election has happened, ballot check will evaluate to false
          # so safe?
          
          @ebal = baln #save election ballot globally
          @eval = proposedlog #save proposed log value globally
          @etal = 0 #election tally - start it over
                      #send to all
            
          sendall(msg)
        
        end # if leader
      
      end #end if u
      
      #sleep(3)
     } # end run loop
      
   end # end def run
   
   # do this stuff when message incoming
   def listen 
    
      begin 
    
       loop{
       
         new_connection = @lsock.accept
        
         Thread.start(new_connection) do |conn| # open thread for each accepted connection
          
            #print "listen - new connection started \n"
          
            loop {
             
              rmsg = conn.gets.chomp
              print "listen - message received: \"#{rmsg}\"\n"
              
              msg = rmsg.split(',')
              tag = msg[0]
              rlog = msg[3]
              spid = msg[5].to_i #since this is a pid
              
              case tag
              
              when "heartbeat"
                  @leaderpid = spid
              #    puts "Heartbeat recvd from #{spid}"
              
              when "request" # I'm leader, someone has a new log value for me to commit
                # send "tellaccept" to all, wait to hear majority of acks from all
                ### ***need logic to check if enough tickets are left to grant this request
                
                #proposedlog = "#{logi}:Kiosk #{@pid} sold #{ntix} tickets"
                #msg = "request,0,0,#{proposedlog},0,#{@pid}"
                ntix=rlog.split(' ')[3].to_i #get num tix
                
                if Tix::get_value - ntix <= 0
                    puts "NOOOOOOOOO NOT ENOUGH TICKETS!"
                    @sendq << msg[5].to_i
                    @msgq[msg[5].to_i] << "unable"
                    next
                else
                    Tix::reduce(ntix)
                    puts "----------------------Available tickets are #{Tix::get_value}"
                end
                
                #rlog = msg[3] # defined above
                
                sendmsg = "tellaccept,0,0,#{rlog},0,#{@pid}"
                
                sendall(sendmsg)
                
                # @allpids.each do |opid|
          
                #   if opid == @pid then next end #own pid
          
                #   @sendq << opid
                #   @msgq[opid] << sendmsg 
        
                # end #allpids loop
                
              when "unable"
                puts "NOOOOOOOOO NOT ENOUGH TICKETS"    
              
              when "tellaccept" # when I'm a follower and leader sends a new message  
                # can always trust a tellaccept
                # commit the value!
                
                # need to check ballot here? if (baln >= @lastbaln)||((baln <= @lastbaln)&&(balpid > @lastbalpid))
                # then we'd need to write ballots into normal operation requests
                
                @leaderpid = spid # ***what if a faulty leader sends a tellaccept?)
                
                updatelog(rlog)
                sendmsg = "ack,0,0,#{rlog},0,#{@pid}"
                
                # put sendmsg into q to return to sender
                @sendq << spid 
                @msgq[spid] << sendmsg
                
              when "ack"
                #if I'm already leader, this is for sure
                if @leaderpid == @pid 
                  updatelog(rlog) 
                  print "listen - received ack from #{spid}, I'm the leader, so update my log\n"
                
                else
                  #otherwise, tally acks here to find majority
                  print "listen - tallying acks here\n"
                  
                  if rlog == @eval # check ack is for my bal and val
                  
                    # increment tally
                    @etal += 1
                    maj = 2 #getlive #numblo&er of live connections / 2 rounded up ### get live array back up
                    
                    if @etal >= maj
                      # I've won the election, 
                      # send "tellaccept to everyone"
                      # send that I'm the leader (everyone should know)
                      
                      print "listen - I've won the election for msg: #{@eval}"
                      
                      @leaderpid = @pid
                      msg = "tellaccept,0,0,#{rlog},0,#{@pid}"
                      
                      sendall(msg)
                      # @allpids.each do |opid|
          
                      #   if opid == @pid then next end #own pid
          
                      #   @sendq << opid
                      #   @msgq[opid] << sendmsg 
        
                      # end #allpids loop
                    
                    end
                  
                  end #if val/bal  
                  
                  # can assume I'll only start one election at a time so any
                  # acks I receive as not leader can only be for my election
                  
                end
                
               when "prepare"
                # someone got an input and is starting an election
                # check ballot
                balstr = bal.split(':')
                baln = balstr[0].to_i
                balpid = balstr[1].to_i 
                
                print "listen - started prepare logic\n"
                print " this baln: #{baln}\n lastbaln: #{@lastbaln}\n this pid: #{spid}\n lastpid: #{@lastbalpid}\n"
                
                #if x or !x & y
                
                if (baln >= @lastbaln)||((baln == @lastbaln)&&(balpid > @lastbalpid))
                  
                  print "listen - new ballot is highest so sending back ack \n"
                  # highest bal seen, accept with ack
                  lastbal = "#{@lastbaln}:#{@lastbalpid}"
                  sendmsg = "ack,#{bal},#{lastbal},#{rlog},#{@lastval},#{@pid}"
                  
                  #send back to election-starter
                  @sendq << spid
                  @msgq[spid] << sendmsg 
                  
                  #then update last highest ballot
                  @lastbaln = baln
                  @lastbalpid = balpid
                  
                end #end if bal  
                  
                
              end #end case tag
              #when tag == "prepare" #can send to all, it's easier # don't respond if bal is < last seen bal
              #   bal = msg[1]
              #   val = msg[3]  # val is most generic value sent in msg
              
              # when "ack" # tally until you get a majority
              #   bal  = msg[1]
              #   obal = msg[2] # obal is the participant's last accepted bal
              #   val  = msg[3]
              #   oval = msg[4] # oval is the participant's last accepted val
              
              # when "queryaccept"
              #   bal = msg[1]
              #   val = msg[3]  # val is most generic value sent in msg
              
              
              # when "accept"
              #   bal = msg[1]
              #   val = msg[3]  # val is most generic value sent in msg
              
              
              # when "tellaccept" #may not be necessary - this is Phase III
              #   bal = msg[1]
              #   val = msg[3]  # val is most generic value sent in msg
             
              
            }
            
            ### PAXOS listening:
            
            # so all msgs are "tag, bal, obal, val, oval, senderpid" with obal and oval set to 0 if irrelevant. 
            
            # if msg = "prepare,    bal, 0,    val, 0,    senderpid"    -> I'm a participant, someone else has a new value to commit
            
            # if msg = "ack,        bal, obal, val, oval, senderpid" -> I've kicked off a ballot and I'm listening for responses
            
            # if msg = "askaccept,  bal, 0,    val, 0,    senderpid"    -> I'm a participant, enough of us have acked a new value from someone
             
            # if msg = "accept,     bal, 0,    val, 0,    senderpid"    -> I've heard from a maj and asked them to accept, they are starting to accept
            
            # if msg = "tellaccept, bal, 0,    val, 0,    senderpid"    -> I'm a participant, Someone got a maj accept, telling everyone. 
            
            # if msg = "request,    0,   0,    val, 0,    senderpid" 
                                              # = #:tickets sold by whom'
            
            
            
            
          end # end thread
        
  
        } # end loop
    
      rescue Exception => e
     
       print "#{e.message}\n"
      
      end # end rescue  
      
      print "listen ends"
   end # end def listener
 
   def sender
     
      begin 
            
            loop{ # do forever
               
              if !@sendq.empty?  # !msgq.empty? - easier to check sendq since single array than msgq which is matrix
                opid = @sendq.shift # then sendq must not be empty either
                #print "sender - found a message to send to #{opid}\n"
                  # get next msgs in the send queue
                  ### check msg at top of q
                
                #msg = @msgq[opid][0] # msgq is a matrix, drop msg into end of q for each pid
                msg = @msgq[opid].shift
                #puts "------------------MSGQ: #{msg}"
                  # get pointer to latest msg for this other, but don't remove yet because send might fail
                #print "sender - msg for #{opid} is: \n  '#{msg.chomp}'\n"
              
                if @allstates[opid] # if the target other is live,
                
                  @allsocks[opid].puts msg 
                  #print "sender - msg sent to #{opid} \n"  
                    
                  @msgq[opid].shift ## remove sent msg from top of q
                  #print "sender - msg removed from q\n"
                 
                else #other socket not connected, leave msg in q
                  hb_check = msg.split(',')
                  if hb_check[0]=="heartbeat"
                      next
                  end
                  # put opid at and of sendq to try again later (= random amoutn of time later)
                  # it's ok to leave msg in first place of q, because even if some other
                  # thread wanted to send that pid a msg, their msg would still need
                  # to arrive after this one does. So even if the pid from that entry 
                  # comes around next in sendq, our msg still gets sent first.
                  # @sendq << opid
                  #print "sender - send to #{opid} failed, requeueing\n"
                
                end # end if allstates
                
              end # end if msgq is not empty
            #sleep(3)  
            }
         #end
         
      rescue Exception => e
          
          print "sender - error with #{e.message}\n"
          # e.backtrace
         
      end # end begin
   end # end def sender
   
   def heartbeat
       loop{
       if @leaderpid == @pid
           sleep 5
           msg = "heartbeat,0,0,0,0,#{@pid}"
           @allpids.each do |opid|
            if opid == @pid then next end #own pid
            @sendq << opid
            @msgq[opid] << msg 
         end
       end
    # send heartbeat - does this need to be the transmitter?
    ### might be useful - have the hb method start whenever we are leader
    # and have it send ids to all procs. 
    # Then procs know leader is live and who to send msgs to
    # dont NEED heartbeat - Amr said. Optional and sometimes useful
       }
   end # end heartbeat
   
   def connect # this might be the heartbeat
     
     loop { # do forever:
    
      ###
      # take latest message out of the q and see whose PID it is
      ###
      
       sleep(2)
       @total = 0
       @majority = 0
       @allpids.each do |opid| # for each other known kiosk,
         
          ###
          # for each other kiosk, if the msg's pid is for this kiosk, 
          # see if kiosk is up and send it
          # (or try to bring it up, then send it)
          # (if kiosk still fails to connect, put msg back in end of q)
          # (Ensures all kiosks may eventually get messages destined for them)
          # (But if leader crashes, do we really want to send a rebooted leader
          # messages that may have already been carried out by previously elected
          # leader?)
          ###
         
          if opid == @pid # if own port, skip
             #puts "Own port is #{opid}, skipping"
             next
          end #end if oport
         
          ohost = @allhosts[opid]
          oport = @allports[opid]
          ostate = @allstates[opid]
         
          #print "ohost: #{ohost} \n oport: #{oport} \n opid: #{opid} \n ostate: #{ostate} \n"
          #puts "ostate is #{ostate} for kiosk #{opid}"
          #if ostate == false # if state is unconnected, try connecting
            #print "connect - kiosk #{opid} not connected, trying now \n"
            
            begin
            
              @allsocks[opid] = TCPSocket.open(ohost, oport)
              
                  # try opening socket
                  # if successful, log config update with a config update log msg - so start a new log entry with 
                  # log[x] = 'Kiosk k is added to the configuration'
                  
            rescue Errno::ECONNREFUSED => e
            
              #puts "got an error"
              #puts e.message
              err = e.message
              err_port = err.split(" ")[7].to_i
              #puts "---------------------Port not connected is: #{err_port}"
              err_pid = @allports.index(err_port)
              if err_pid == @leaderpid
                  puts "-----------------LEADER DOWN!"
              end
              
              #print "connect - connection to #{opid} failed\n"
              
              ostate = false # keep connection state at false
              
              ### if it's the leader who is not responding and we have a message
              # to send it, don't just put the message into the back of the q.
              # find a new leader and send the message there. (so never put the message
              # to the back of the q? So that )
              ###
            
            else # no errors thrown so
              @total += 1
            
              #print "connect - kiosk #{opid} successfully connected \n"
              ostate = true # set connection state to true
            
            end # end begin
          
          @allstates[opid] = ostate
          
        end #end allpids each loop
        @total += 1
        @majority = @total/2 + 1
        #puts "Total connected are #{@total}"
        #puts "Majority is #{@majority}"
        
      }
   end # end def connect

   def updatelog(logmsg) #just the last entry of the paxos messages sent over the channel
     lmsg = logmsg.split(':') # expecting entry#:"text of entry"
     @log[lmsg[0].to_i] = lmsg[1].to_s
     logfile="log#{@pid}.log"
     if @leaderpid == @pid
        if @logcount == 0
            @logcount += 1
            f1 = File.open(logfile,'a')
            f1.puts lmsg[1].to_s
            f1.close
        else
            @logcount += 1
            if @logcount == @total - 1
                @logcount = 0
            end
        end
     else
        f1 = File.open(logfile,'a')
        f1.puts lmsg[1].to_s
        f1.close
     end
     
     #print "Updating #{@pid}'s log with 'log[#{lmsg[0]}] = '#{lmsg[1]}'\n"
   end # end UpdateLog
   
   def sendall(sendmsg)
       @allpids.each do |opid|
          
          if opid == @pid then next end #own pid
          
          @sendq << opid
          @msgq[opid] << sendmsg 
        
        end #allpids loop
   end #end def sendall   

end # end Client


pid = ARGV.shift.to_i # receive kiosk PID from initialization


# prepare array of arrays to of other clients
# [[hosts],[ports],[socketopen?(define this later)]]
conf = Array.new() {Array.new()}
all_hosts = []
all_ports = []
all_pids = []
i = 0 # define conf iterator

# read config file
confname = "kiosks.conf"
    
f = File.open(confname, "r")
f.each_line do |line|
  conf[i] = line.split(" ")
  i += 1
end
f.close

# convert port to integer and populate other_ port and all_pids
for j in 0..i-1
  conf[j][1] = conf[j][1].to_i
  
  # save conf into respective lists (including own)
  all_hosts << conf[j][0]
  all_ports << conf[j][1]
  all_pids << j
  
end

### Read in log here

log = Array.new()

newlog = "log#{pid}.log"
if File.exist?(newlog)
    puts "Loading log..."
    l = File.open(newlog, "r")
    l.each_line do |line|
        log << line
    end
    l.close
else
    File.new(newlog,'a')
    f1=open(newlog,'a')
    f1.puts "Initializing log"
    f1.close
end

logname = "log.log"
l = File.open(logname, "r")
l.each_line do |line|
  log << line
end
l.close

print "Main: creating kiosk #{pid} now \n"

Client.new(all_hosts, all_ports, all_pids, pid, log)



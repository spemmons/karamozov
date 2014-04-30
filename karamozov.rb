require 'logger'
require 'redis'
require 'singleton'
require 'yaml'

class Karamozov

  include Singleton

  CONDUCTOR_DUTY  = 'conductor'
  ROUTER_DUTY     = 'router'
  WORKER_DUTY     = 'worker#'

  # REDIS BEHAVIOR

  def redis
    @redis ||= Redis.new
  end

  def process_hash_key;   @process_hash_key   ||= "#{self.class}:processes";      end
  def depth_hash_key;     @depth_hash_key     ||= "#{self.class}:depths";         end
  def worker_hash_key;    @worker_hash_key    ||= "#{self.class}:workers";        end
  def count_hash_key;     @count_hash_key     ||= "#{self.class}:counts";         end

  def need_list_key;      @need_list_key      ||= "#{self.class}:needs";          end
  def conductor_list_key; @conductor_list_key ||= duty_list_key(CONDUCTOR_DUTY);  end
  def router_list_key;    @router_list_key    ||= duty_list_key(ROUTER_DUTY);     end

  def duty_list_key(duty_name)
    "#{self.class}:duty_#{duty_name}"
  end

  # RUN/DUTY BEHAVIOR

  def run(action,args = [])
    case action
      when /\Acheck\Z/i then  run_check
      when /\Astart\Z/i then  run_start
      when /\Astop\Z/i  then  run_stop
      when /\Aspawn\Z/i then  run_spawn(args)
      when /\Aflush\Z/i then  run_flush
      when /\Aduty\Z/i  then  run_duty
      when /\Ahelp\Z/i  then  run_help
      else                    run_help
    end

  rescue
    log_error($!,$@)

  ensure
    terminate_duty
  end

  def run_check
    duty_pattern = /#{duty_list_key('(.*)')}/
    possible_duties = ([CONDUCTOR_DUTY,ROUTER_DUTY] + redis.hkeys(process_hash_key) + redis.keys(duty_list_key('*')).collect{|list_name| (list_name =~ duty_pattern) && $1}.compact).uniq.sort
    check_notes = possible_duties.collect do |duty_name|
      "#{(duty_name =~ /((c)onductor|(r)outer|(w)orker(\d+))/) ? ($2 || $3 || "#{$4}#{$5}").upcase : duty_name}:#{redis.hget(process_hash_key,duty_name) ? 'T' : 'F'},#{redis.llen(duty_list_key(duty_name))}"
    end
    check_notes << "N:#{redis.llen(need_list_key)}"
    log_info_and_echo((%w(CHECK -) + check_notes).join(' '))
  end

  def run_start
    if check_process(conductor_process)
      log_info_and_echo('Already started')
    else
      act_as_conductor
    end
  end

  def run_stop
    return log_info_and_echo('Not running') unless (kill_conductor = check_process(conductor_process)) or existing_duty_keys.any?

    if kill_conductor
      log_info_and_echo('Sending stop to conductor')
      redis.lpush(conductor_list_key,stop_action)
    else
      log_info_and_echo('Stopping without conductor!')
      act_as_stopper
      log_info_and_echo('Stopped')
    end
  end

  def run_spawn(args)

  end

  def run_flush
    if check_process(conductor_process)
      log_info_and_echo('Still running!')
    else
      act_as_conductor(false)
    end
  end

  def run_help
    puts 'command line arguments...'
    puts '- check: list a status summary of all duties'
    puts "- start: start the conductor, router, and worker duties based on settings in #{log_filename}"
    puts '- stop:  stop all running duties'
    puts '- flush: starts the conductor and worker duties needed to empty any queues'
    puts '- help:  display this list'
  end

  def run_duty
    log_info('Reporting for duty...')
    key,duty = redis.brpop(need_list_key,need_timeout)
    return log_warn('Process duty not found') unless key == need_list_key

    case duty
      when ROUTER_DUTY
        act_as_router
      when /\Aworker\d+\Z/
        act_as_worker(duty)
      else
        raise "Process #{duty} request unknown"
    end
  end

  def conductor_process
    redis.hget(process_hash_key,CONDUCTOR_DUTY)
  end

  def router_process
    redis.hget(process_hash_key,ROUTER_DUTY)
  end

  def check_process(process)
    host,pid = process.split(',') if process
    host && (host != Socket.gethostname || Process.getpgid(pid.to_i))
  rescue
    log_error($!,$@)
    nil
  end

  def establish_duty(duty)
    raise "Process #{duty} already exists" if check_process(redis.hget(process_hash_key,duty))
    raise "Process #{duty} failed to establish" unless redis.hsetnx(process_hash_key,duty,process = [Socket.gethostname,Process.pid].join(','))

    $0 = "#{$0} #{duty}"
    @duty,@process = duty,process
    log_warn('Duty established')
    @process
  end

  def terminate_duty
    return unless @duty

    raise 'Duty is not self' unless @process == redis.hget(process_hash_key,@duty)

    log_warn('Duty terminated')
    redis.hdel(process_hash_key,@duty)
    @duty,@process = nil,nil
  end

  # CONDUCTOR BEHAVIOR

  def act_as_conductor(run_router = true)
    logger.warn '---------------------------------------------------------------------------------------------'

    redis.hdel(process_hash_key,CONDUCTOR_DUTY)
    establish_duty(CONDUCTOR_DUTY)

    kill_duties unless check_process(router_process)

    # TODO reintroduce a missing conductor?
    raise "Not sure what do to with situation: #{existing_process_keys.inspect}" unless existing_process_keys == [CONDUCTOR_DUTY]

    if run_router
      flush_hash(depth_hash_key)
      flush_hash(worker_hash_key)
      flush_hash(count_hash_key)
      @duty_keys = [ROUTER_DUTY] + (1..max_workers).collect{|index| WORKER_DUTY.gsub(/#/,'%02d' % index)}
    else
      worker_pattern = /(#{WORKER_DUTY.gsub(/#/,'')}\d+)/
      @duty_keys = redis.keys(duty_list_key(WORKER_DUTY).gsub(/#/,'*')).collect{|list_name| (list_name =~ worker_pattern) && $1}.compact.sort
      log_info("remaining workers: #{@duty_keys}")
    end

    flush_list(need_list_key)
    flush_list(router_list_key)
    return log_warn('No duties found') if @duty_keys.empty?

    fill_list(need_list_key,@duty_keys)

    # TODO user spawner...
    #log_info("Duties to fill: #{redis.llen(need_list_key)}")
    #@duty_keys.length.times{`bundle exec ruby #{$0} #{$*.join(' ')}`}
    #while (remaining = redis.llen(need_list_key)) > 0
    #  log_info("Duties remaining: #{remaining}")
    #  sleep(1)
    #end
    #log_info('Duties filled')

    @last_timestamp = Time.now
    @last_counts = redis.hmget(count_hash_key,@duty_keys)
    while perform_conductor_action
      log_stats
      break unless run_router or @backlog.detect{|queue_depth| queue_depth.to_i > 0}
    end

    log_info('Stopping...')
    act_as_stopper
  end

  def act_as_stopper
    flush_list(need_list_key)
    flush_list(router_list_key)
    kill_duties
  end

  def kill_duties
    existing_duty_keys.each{|duty_name| redis.rpush(duty_list_key(duty_name),stop_action)}

    while (process_keys = existing_duty_keys).length > 0
      log_info("Waiting for duties to stop: #{process_keys}")
      sleep(kill_timeout)
    end
  end

  def flush_hash(key)
    redis.hkeys(key).each{|field| redis.hdel(key,field)}
  end

  def fill_list(key,values)
    values.each{|value| redis.lpush(key,value)}
  end

  def flush_list(key)
    redis.rpop(key) while redis.llen(key) > 0
  end

  def perform_conductor_action
    _,action = redis.brpop(duty_list_key(@duty),stats_period)
    case action
      when 'stop'       then  false
      when nil          then  true
      else                    action
    end
  end

  def existing_duty_keys
    existing_process_keys - [CONDUCTOR_DUTY]
  end

  def existing_process_keys
    Array(redis.hkeys(process_hash_key)).inject([]) do |result,field|
      if process = redis.hget(process_hash_key,field)
        if check_process(process)
          result << field
        else
          redis.hdel(process_hash_key,field)
        end
      end
      result
    end
  end

  def log_stats
    return unless (current_period = ((current_timestamp = Time.now) - @last_timestamp)) > stats_period

    stats = []
    current_counts = redis.hmget(count_hash_key,@duty_keys)
    @last_counts.each_with_index do |last_count,index|
      current_count = current_counts[index]
      stats[index] = current_count && last_count ? sprintf('%0.1f',(current_count.to_f - last_count.to_f) / current_period) : '-'
    end

    log_info_and_echo(%(STATS:   #{stats.join("\t")}))

    @backlog = redis.eval(%(return {#{@duty_keys.collect{|duty| "redis.call('llen','#{duty_list_key(duty)}')"}.join(',')}}),[],[])
    log_info_and_echo(%(BACKLOG: #{@backlog.join("\t")}))

    @last_timestamp = current_timestamp
    @last_counts = current_counts
  end

  # ROUTER BEHAVIOR

  def act_as_router
    establish_duty(ROUTER_DUTY)

    redis.hset(count_hash_key,ROUTER_DUTY,0)

    while redis.lindex(router_list_key,-1) != stop_action
      log_debug('Checking for message...')
      next unless message = next_router_message

      log_debug('Checking for worker...')
      while (worker = choose_worker_for_source(source = identify_source_for_message(message))).nil?
        log_debug('No worker found; now what!') # TODO figure out what to do here...
      end

      break if worker == stop_action

      log_debug("Route to #{worker}")
      redis.hincrby(depth_hash_key,source,1) if source
      redis.lpush(duty_list_key(worker),message)
      redis.hincrby(count_hash_key,ROUTER_DUTY,1)
    end
  end

  def choose_worker_for_source(source)
    unless source and redis.hget(depth_hash_key,source).to_i > 0 and (worker = redis.hget(worker_hash_key,source))
      while true
        _,worker = redis.brpop(router_list_key,queue_timeout)

        if worker and redis.llen(duty_list_key(worker)) > 0
          log_warn("Process #{worker} is busy; put it back in the ready list!")
          redis.lpush(router_list_key,worker)
        elsif worker
          redis.hset(worker_hash_key,source,worker)
          break
        end
      end
    end
    worker
  end

  def next_router_message
    raise 'No next_router_message defined'
  end

  def identify_source_for_message(message)
  end

  # WORKER BEHAVIOR

  def act_as_worker(duty)
    establish_duty(duty)

    redis.hset(count_hash_key,@duty,0)

    worker_list_key = duty_list_key(@duty)
    ready_notified = false
    while true
      if ready_notified
        _,action = redis.brpop(worker_list_key,queue_timeout)
      else
        action = redis.rpop(worker_list_key)
      end
      case action
        when stop_action
          break

        when nil
          if ready_notified
            log_info('No message...')
          elsif redis.llen(worker_list_key) > 0
            log_warn('No message, but queue is not empty!')
          else
            log_debug('Ready...')
            redis.lpush(router_list_key,@duty)
            ready_notified = true
          end

        else
          ready_notified = false
          serialize_message_source(action)
      end
    end
  end

  def serialize_message_source(message)
    log_debug('Message start')
    source = identify_source_for_message(message)
    process_worker_message(message)
    redis.hincrby(count_hash_key,@duty,1)
    redis.hincrby(depth_hash_key,source,-1) if source
    log_debug('Message stop')
  end

  def process_worker_message(message)
  end

  # SETTINGS BEHAVIOR

  def stop_action;    @stop_action    ||= settings['stop_action']    || 'stop'; end
  def need_timeout;   @need_timeout   ||= settings['need_timeout']   || 10;     end
  def queue_timeout;  @queue_timeout  ||= settings['queue_timeout']  || 15;     end
  def kill_timeout;   @kill_timeout   ||= settings['kill_timeout']   || 5;      end
  def stats_period;   @stats_period   ||= settings['stats_period']   || 60;     end
  def max_workers;    @max_workers    ||= settings['max_workers']    || 1;      end

  def settings
    @settings ||= load_settings || {}
  end

  def load_settings
    return unless File.exists?(settings_filename)

    result = YAML.load_file(settings_filename)
    defined?(Rails) ? result[Rails.env] : result

  rescue
    log_error($!,$@)
    nil
  end

  def settings_filename
    @settings_filename ||= (defined?(Rails) ? 'config/%s.yml' : '%s.yml') % base_filename
  end

  # LOGGING BEHAVIOR

  def log_info_and_echo(message)
    log_info(message)
    puts message
  end

  def log_debug(message)
    logger.debug "#{@duty || 'pending'} - #{message}"
  end

  def log_info(message)
    logger.info "#{@duty || 'pending'} - #{message}"
  end

  def log_warn(message)
    logger.warn "#{@duty || 'pending'} - #{message}"
  end

  def log_error(message,trace = nil)
    logger.error %(#{@duty || 'pending'} - ERROR: #{message}#{trace && ([nil] + trace).join("\n")})
  end

  def logger
    @logger ||= new_logger
  end

  def new_logger
    file = File.open(log_filename,'a')
    file.sync = true
    logger = Logger.new(file)
    logger.level = settings['log_level'] || Logger::INFO
    logger
  end

  def log_filename
    @log_filename ||= (defined?(Rails) ? 'log/%s.log' : '%s.log') % base_filename
  end

  def base_filename
    @base_filename ||= self.class.to_s.gsub(/([A-Z])/,'_\1').downcase.gsub(/^_/,'')
  end

end
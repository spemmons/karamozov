load 'karamozov.rb'

class Tester < Karamozov

  def act_as_conductor(run_router = true)
    redis.hdel('routed',@routed_keys) if (@routed_keys = redis.hkeys('routed')).any?
    redis.hdel('worked',@worked_keys) if (@worked_keys = redis.hkeys('worked')).any?
    super(run_router)
    log_stats unless @duty_keys.empty?
    @routed_keys = Array(redis.hkeys('routed').sort)
    @worked_keys = Array(redis.hkeys('worked').sort)
    puts "NEVER ROUTED: #{@worked_keys - @routed_keys}"
    puts "NEVER WORKED: #{@routed_keys - @worked_keys}"
    (@routed_keys + @worked_keys).uniq.sort.each{|key| puts [key,routed = redis.hget('routed',key),worked = redis.hget('worked',key),routed.to_i - worked.to_i].join("\t") unless routed.to_i - worked.to_i == 0}
  end

  def identify_source_for_message(message)
    message[0]
  end

  def next_router_message
    @input_file ||= File.open(settings['input_file'])
    @input_file = nil unless message = @input_file.gets
    redis.hincrby('routed',message.chomp!,1) if message
    log_debug("Router message: #{message || 'NONE'} - #{redis.llen(duty_list_key(@duty))}")
    message
  end

  def process_worker_message(message)
    redis.hincrby('worked',message,1) if message
    log_debug("Worker message: #{message} - #{redis.llen(duty_list_key(@duty))}")
    #sleep((message =~ /[a-c]/ ? 0.1 : message =~ /[x-z]/ ? 0.05 : 0.01)*[message.length,1].max)
  end

end

Tester.instance.run($*[0],$*[1..-1])



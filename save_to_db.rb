# -*- coding: utf-8 -*-

require 'mysql2'

Plugin.create :puts_timeline do
  @client = Mysql2::Client.new(:host => "localhost", :username => "mikutter", :password => "", :database => "mikutter")
  filter_update do |service, msgs|
    msgs.each do |m| 
        ca = m["created_at"]
        id = m[:id]
        mess = @client.escape(m[:message])
        sour = @client.escape(m[:source])
        coordinates1 = "NULL" # if m[:coordinates].nil? && false then "NULL" else m[:coordinates]["coordinates"][0] end
        coordinates2 = "NULL" #if m[:coordinates].nil? && false then "NULL" else m[:coordinates]["coordinates"][1] end
        hash =if m[:entities].nil? then '' else m[:entities]['hashtags'].to_s end
        eurl = m[:entities]['urls'].to_s
        fc = m[:favorite_count]
        um = m[:entities]['user_mentions'].to_s
        fd = if m[:favorited] then 1 else 0 end
        irtSN =  @client.escape(m[:in_reply_to_screen_name].to_s)
        irtSI = if m[:in_reply_to_status_id].nil? then "NULL" else m[:in_reply_to_status_id] end
        irtUI = if m[:in_reply_to_user_id].nil? then "NULL" else m[:in_reply_to_user_id] end
        rd = if m[:retweeted] then 1 else 0 end
        pd = if m[:user][:protected] then 1 else 0 end
        uid = m[:user][:id]
        idname = @client.escape(m[:user][:idname].to_s)
        name = @client.escape(m[:user][:name].to_s)
        location = @client.escape(m[:user][:location].to_s)
        detail = @client.escape(m[:user][:detail].to_s)
        url = @client.escape(m[:user][:url].to_s)
        qs = "INSERT INTO tweets VALUES ('#{ca}', #{id}, '#{mess}', '#{sour}', #{coordinates1}, #{coordinates2}, '#{hash}', '#{eurl}', '#{um}', #{fc}, #{fd}, '#{irtSN}', #{irtSI}, #{irtUI}, '#{m[:lang]}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, #{m[:retweet_count]}, #{rd}, '#{m[:user]}', #{pd}, #{uid}, '#{idname}', '#{name}', '#{location}', '#{detail}', '#{url}', #{m[:user][:followers_count]}, #{m[:user][:statuses_count]}, #{m[:user][:friends_count]});"
	begin 
          @client.query(qs)
	rescue Exception => e
	  p e
	end
    end
    [service, msgs]
  end
#  on_appear do |messages|
#    messages.each do |m|
#    end
#  end

  filter_delete do |m|
    # mikutter/core/plugin/streaming/streamer.rb の event_factory の when json['delete'] に Plugin.filtering(:delete, json['delete']) を追記
    qs = "INSERT INTO deleted VALUES (#{m['status']['id']}, #{m['status']['user_id']});";
    @client.query(qs)
  end
end




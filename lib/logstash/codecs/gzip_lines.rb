# encoding: utf-8
require "logstash/codecs/base"
require "logstash/codecs/plain"
require "logstash/json"
require "logstash/event"
require "zlib"
require "stringio"

# This codec will read gzip encoded content
class LogStash::Codecs::GzipLines < LogStash::Codecs::Base
  config_name "gzip_lines"


  # The character encoding used in this codec. Examples include "UTF-8" and
  # "CP1252"
  #
  # JSON requires valid UTF-8 strings, but in some cases, software that
  # emits JSON does so in another encoding (nxlog, for example). In
  # weird cases like this, you can set the charset setting to the
  # actual encoding of the text and logstash will convert it for you.
  #
  # For nxlog users, you'll want to set this to "CP1252"
  config :charset, :validate => ::Encoding.name_list, :default => "UTF-8"

  public
  def initialize(params={})
    super(params)
    @converter = LogStash::Util::Charset.new(@charset)
    @converter.logger = @logger
  end

  def from_json_parse(json)
    LogStash::Event.from_json(json).each { |event| yield event }
  rescue LogStash::Json::ParserError => e
    @logger.error("JSON parse error, original data now in message field", :error => e, :data => json)
    yield LogStash::Event.new("message" => json, "tags" => ["_jsonparsefailure"])
  end

  public
  def decode(data)
    data = StringIO.new(data) if data.kind_of?(String)

    @decoder = Zlib::GzipReader.new(data)

    begin
      decompressed_data = @decoder.read
      from_json_parse(decompressed_data)
    rescue Zlib::Error, Zlib::GzipFile::Error=> e
      @logger.debug("Gzip codec: We cannot uncompress the gzip data", :data => data)

      # if failed to decompress, try json parsing original data
      from_json_parse(data)
    end
  end # def decode
end # class LogStash::Codecs::GzipLines

File::open('dummy.log', 'w') { |f|
  (1..1000000).each {
    f.puts(Time.now.strftime('%Y-%m-%d %H:%M:%S.%L'))
  }
}

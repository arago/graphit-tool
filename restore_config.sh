test -f /etc/graphit-tool.conf.rpmsave || exit 0

if cmp --silent /etc/graphit-tool.conf /etc/graphit-tool.rpmsave
then
  rm /etc/graphit-tool.rpmsave
else
  mv /etc/graphit-tool.conf /etc/graphit-tool.conf.rpmnew
  mv /etc/graphit-tool.rpmsave /etc/graphit-tool.conf
fi

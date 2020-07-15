package ssh

//tunnel usage:
//  sshTool, err := NewSSHTool(conf.BoardHost, conf.UserName, conf.Password, conf.PrivatePath)
//	if err != nil {
//
//	}
//	proxyServer, err := NewTunnelServer(ctx.RootCtx, localAddr, remoteAddr, sshTool)
//	if err != nil {
//
//	}
//	//run ssh proxy
//	go proxyServer.Start()

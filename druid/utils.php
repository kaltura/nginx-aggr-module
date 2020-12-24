<?php

class Logger
{
    protected $handle;
    protected $uid;

    public function __construct($filePath)
    {
        $this->handle = fopen($filePath, 'a');
        $this->uid = (string) rand();
    }

    public function write($msg)
    {
        fwrite($this->handle, date('Y-m-d H:i:s') . ' [' . $this->uid . '] ' . $msg . "\n");
    }

    public function info($msg)
    {
        $this->write('Info: ' . $msg);
    }

    public function error($msg)
    {
        $this->write('Error: ' . $msg);
    }

	public function dieError($msg)
	{
		$this->error($msg);
		die;
	}

    public function __destruct()
    {
        fclose($this->handle);
    }

    public function getUid()
    {
        return $this->uid;
    }
}


function postJson($url, $query, $logger)
{
    $data = json_encode($query);

    $logger->info("query $url body $data");

    $ch = curl_init($url);

    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_ENCODING, 'gzip,deflate');

    curl_setopt($ch, CURLOPT_HTTPHEADER, array('Content-Type: application/json'));
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $data);

    $response = curl_exec($ch);
    $errno = curl_errno($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);

    $logger->info("response $response errno $errno code $httpCode");

    return $response;
}

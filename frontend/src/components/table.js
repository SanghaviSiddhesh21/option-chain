import React, { useEffect, useState } from 'react';
import TableTop from './tabletopHeader';
// import Dropdown from './dropdown'
import '../styles/styles.css';
import { UseSelector, useSelector } from 'react-redux';
import { baseUrl } from '../constants';
const OptionTable = () => {
    const [data, setData] = useState([]);
    const query = useSelector(state => state.app.symbolAndExpiry)

    useEffect(() => {
        let prevData = [];
        const fetchData = async () => {
            try {
                console.log(query);

                let url;
                // if (props.selSymbol && props.selExpiry) {
                // url = 'http://127.0.0.1:8080/api/' + props.selSymbol + '+' + props.selExpiry;
                // } else {
                // url = `${baseUrl}/symbol_date_option/${query}`;
                if (query == '') {
                    url = `https://caf7-2401-4900-57eb-86db-2c9d-a1b-d4de-4106.in.ngrok.io/api/mount_options`;
                } else {
                    url = `${baseUrl}/symbol_date_option/${query}`;
                }






                const response = await fetch(url);
                const jsonData = await response.json();


                // Calculate the class name for changed cells
                const updatedData = jsonData.data.map((item) => {
                    const prevItem = prevData.find((prev) => prev.strike_price === item.strike_price);
                    const className = prevItem && prevItem.ltp_c !== item.ltp_c ? 'highlight' : '';
                    return { ...item, className };
                });

                setData(updatedData);
                prevData = jsonData.data;

                // Remove the highlight after 3 seconds
                setTimeout(() => {
                    setData((prevData) =>
                        prevData.map((item) => {
                            return { ...item, className: '' };
                        })
                    );
                }, 3000);
            } catch (error) {
                console.error('Error fetching data:', error);
            }
        };

        fetchData();

        const interval = setInterval(fetchData, 1000);

        return () => {
            clearInterval(interval);
        };
    }, [query]);
    return (
        <>
            <TableTop />
            <div className="table-structure">
                <div className="table-container">
                    <table className="table">
                        <thead>
                            <tr>
                                <th>OI</th>
                                <th>OI Qty</th>
                                <th>Volume</th>
                                <th>IV</th>
                                <th>Bid Price</th>
                                <th>Bid Qty</th>
                                <th>LTP</th>
                                <th>LTP +/-</th>
                                <th>Ask Price</th>
                                <th>Ask Qty</th>
                                <th><span className="strike-price">Strike Price</span></th>
                                <th>Bid Price</th>
                                <th>Bid Qty</th>
                                <th>LTP</th>
                                <th>LTP +/-</th>
                                <th>Ask Price</th>
                                <th>Ask Qty</th>
                                <th>IV</th>
                                <th>Volume</th>
                                <th>OI Qty</th>
                                <th>OI</th>
                            </tr>
                        </thead>
                        <tbody>
                            {data.length > 0 ? (
                                data.map((item, index) => (
                                    <tr key={index}>
                                        <td className={item.className}>{item.oi_c}</td>
                                        <td style={{ color: item.oi_change_c > 0 ? '#1A936F' : '#FF0000' }} className={item.className}>{item.oi_change_c}</td>
                                        <td className={item.className}>{item.volume_c}</td>
                                        <td className={item.className}>{item.iv_c}</td>
                                        <td className={item.className}>{item.bid_c}</td>
                                        <td className={item.className}>{item.bidqty_c}</td>
                                        <td className={item.className}>{item.ltp_c}</td>
                                        <td style={{ color: item.change_c > 0 ? '#1A936F' : '#FF0000' }} className={item.className}>{item.change_c}</td>
                                        <td className={item.className}>{item.ask_c}</td>
                                        <td className={item.className}>{item.askqty_c}</td>
                                        <td className={item.className}>{item.strike_price}</td>
                                        <td className={item.className}>{item.bid_p}</td>
                                        <td className={item.className}>{item.bidqty_p}</td>
                                        <td className={item.className}>{item.ltp_p}</td>
                                        <td style={{ color: item.oi_change_p > 0 ? '#1A936F' : '#FF0000' }} className={item.className}>{item.change_p}</td>
                                        <td className={item.className}>{item.ask_p}</td>
                                        <td className={item.className}>{item.askqty_p}</td>
                                        <td className={item.className}>{item.iv_p}</td>
                                        <td className={item.className}>{item.volume_p}</td>
                                        <td style={{ color: item.oi_change_p > 0 ? '#1A936F' : '#FF0000' }} className={item.className}>{item.oi_change_p}</td>
                                        <td className={item.className}>{item.oi_p}</td>
                                    </tr>
                                ))
                            ) : (
                                <tr>
                                    <td colSpan="22">No data available</td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>
            </div>
        </>
    );
}

export default OptionTable;
// import '../styles/styles.css'


// function Banner() {
//     return (
//         <div className='banner-main-text'>
//             <text className="banner-text-left">
//                 Option Chain (Equity Derivatives)
//             </text>

//             {/* The API will fetch the current underlying NIFTY price over here */}

//             <div className='test'>
//                 <label className='banner-text-right'> Underlying Index: <span className='blue-middle-text'>NIFTY </span>19,586.5</label>

//                 {/* The API will fetch the current date over here */}

//                 <label className='sub-text'>
//                     As on
//                 </label>
//             </div>
//         </div>
//     )
// }

// export default Banner



import { useState, useEffect } from 'react';
import '../styles/styles.css';

function Banner() {
    const [indexPrice, setIndexPrice] = useState(null);
    const [indexDataTimestamp, setIndexDataTimestamp] = useState(null);

    useEffect(() => {
        const fetchData = async () => {
            try {
                const response = await fetch('https://caf7-2401-4900-57eb-86db-2c9d-a1b-d4de-4106.in.ngrok.io/api/mount_options');
                const data = await response.json();
                setIndexPrice(data.index_price);
                setIndexDataTimestamp(data.index_data_timestamp);
            } catch (error) {
                console.error('Error fetching data:', error);
            }
        };

        const interval = setInterval(fetchData, 1000);

        return () => {
            clearInterval(interval);
        };
    }, []);

    return (
        <div className='banner-main-text'>
            <text className="banner-text-left">
                Option Chain (Equity Derivatives)
            </text>

            <div className='test'>
                <label className='banner-text-right'>
                    Underlying Index: <span className='blue-middle-text'>NIFTY </span>
                    {indexPrice / 100}
                </label>

                <label className='sub-text'>
                    As on {indexDataTimestamp}
                </label>
            </div>
        </div>
    );
}

export default Banner;
